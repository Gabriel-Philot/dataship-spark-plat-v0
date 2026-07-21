package io.dataship.spark.observer

import java.time.{Clock, Instant}

import io.dataship.spark.observer.api.{CountersResponse, HealthResponse}
import io.dataship.spark.observer.events.{
  BoundedEventQueue,
  ObserverCounters,
  ObserverEvent,
  ObserverListener,
  ObserverState
}

final class ObserverRuntime(
    val config: ObserverConfig,
    val sparkVersion: String,
    clock: Clock = Clock.systemUTC(),
    initialErrorCode: String = ""
) {
  private val supportedRuntime = sparkVersion == BuildInfo.SupportedSparkVersion
  private var applicationId = ""
  private var uiAttached = false
  private var listenerInstalled = false
  private var lastErrorCode = initialErrorCode
  private var stopped = false
  private var status = initialStatus()
  private var ownedEventQueue: Option[BoundedEventQueue] = None
  private var ownedListener: Option[ObserverListener] = None

  private[observer] def eventQueue: Option[BoundedEventQueue] = synchronized {
    if (!config.enabled || stopped) {
      None
    } else {
      if (ownedEventQueue.isEmpty) {
        ownedEventQueue = Some(
          new BoundedEventQueue(
            capacity = config.queueCapacity,
            state = new ObserverState(config.transitionsCapacity),
            process = _ => {
              if (config.testMode && config.testProcessingDelayMs > 0) {
                Thread.sleep(config.testProcessingDelayMs.toLong)
              }
            }
          )
        )
      }
      ownedEventQueue
    }
  }

  def listenerForInstallation: Option[ObserverListener] = synchronized {
    if (!config.enabled || stopped) {
      None
    } else {
      if (ownedListener.isEmpty) {
        ownedListener = eventQueue.map(queue =>
          new ObserverListener(queue = queue, clock = clock)
        )
      }
      ownedListener
    }
  }

  def markListenerInstalled(): Unit = synchronized {
    if (!stopped && config.enabled && ownedListener.nonEmpty) {
      listenerInstalled = true
    }
  }

  def registerApplication(appId: String): Boolean = synchronized {
    val normalizedAppId = Option(appId).map(_.trim).getOrElse("")
    if (stopped) {
      false
    } else if (normalizedAppId.isEmpty) {
      if (applicationId.isEmpty && lastErrorCode.isEmpty) {
        markFailure("EMPTY_APP_ID")
      }
      false
    } else {
      applicationId = normalizedAppId
      if (lastErrorCode == "EMPTY_APP_ID") {
        lastErrorCode = ""
        status = if (config.enabled) "STARTING" else "DISABLED"
      }
      true
    }
  }

  def markUiAttached(): Unit = synchronized {
    if (!stopped) {
      uiAttached = true
      if (lastErrorCode.isEmpty) {
        status = if (config.enabled) "READY" else "DISABLED"
      }
    }
  }

  def markNoSparkUi(): Unit = synchronized {
    if (!stopped) {
      uiAttached = false
      lastErrorCode = "NO_SPARK_UI"
      status = if (config.enabled) "DEGRADED" else "DISABLED"
    }
  }

  def markFailure(errorCode: String): Unit = synchronized {
    if (!stopped) {
      lastErrorCode = errorCode
      status = "DEGRADED"
    }
  }

  def shutdown(): Unit = {
    val queueToClose = synchronized {
      stopped = true
      status = "STOPPING"
      listenerInstalled = false
      ownedEventQueue
    }
    queueToClose.foreach(_.close())
  }

  def isSupportedRuntime: Boolean = supportedRuntime

  def healthResponse: HealthResponse = synchronized {
    HealthResponse(
      schemaVersion = "v1",
      pluginVersion = BuildInfo.PluginVersion,
      sparkVersion = sparkVersion,
      appId = applicationId,
      mode = "live",
      capturedAt = Instant.now(clock).toString,
      status = status,
      uiAttached = uiAttached,
      listenerInstalled = listenerInstalled,
      supportedRuntime = supportedRuntime,
      queueCapacity = config.queueCapacity,
      lastErrorCode = lastErrorCode
    )
  }

  def countersResponse: CountersResponse = synchronized {
    val counters = ownedEventQueue
      .map(_.state.snapshot)
      .getOrElse(ObserverRuntime.emptyCounters)
    CountersResponse(
      schemaVersion = "v1",
      pluginVersion = BuildInfo.PluginVersion,
      sparkVersion = sparkVersion,
      appId = applicationId,
      mode = "live",
      capturedAt = Instant.now(clock).toString,
      receivedByCategory = counters.receivedByCategory,
      listenerReceived = counters.listenerReceived,
      processed = counters.processed,
      queued = counters.queued,
      inFlight = counters.inFlight,
      droppedByPlugin = counters.droppedByPlugin,
      internalFailures = counters.internalFailures,
      depth = counters.queued,
      capacity = config.queueCapacity,
      lastEventAt = counters.lastEventAt.map(_.toString).getOrElse(""),
      invariantHolds = counters.invariantHolds
    )
  }

  private def initialStatus(): String = {
    if (!supportedRuntime) {
      lastErrorCode = "UNSUPPORTED_SPARK_VERSION"
      "DEGRADED"
    } else if (lastErrorCode.nonEmpty) {
      "DEGRADED"
    } else if (config.enabled) {
      "STARTING"
    } else {
      "DISABLED"
    }
  }
}

private object ObserverRuntime {
  val emptyCounters: ObserverCounters = ObserverCounters(
    listenerReceived = 0L,
    processed = 0L,
    queued = 0L,
    inFlight = 0L,
    droppedByPlugin = 0L,
    receivedByCategory = ObserverEvent.Categories.map(_ -> 0L).toMap,
    internalFailures = 0L,
    lastEventAt = None,
    recentEvents = Vector.empty
  )
}
