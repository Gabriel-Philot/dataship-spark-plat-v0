package io.dataship.spark.observer

import java.time.{Clock, Instant}

import io.dataship.spark.observer.api.HealthResponse

final class ObserverRuntime(
    val config: ObserverConfig,
    val sparkVersion: String,
    clock: Clock = Clock.systemUTC(),
    initialErrorCode: String = ""
) {
  private val supportedRuntime = sparkVersion == BuildInfo.SupportedSparkVersion
  private var applicationId = ""
  private var uiAttached = false
  private var lastErrorCode = initialErrorCode
  private var stopped = false
  private var status = initialStatus()

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

  def shutdown(): Unit = synchronized {
    stopped = true
    status = "STOPPING"
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
      listenerInstalled = false,
      supportedRuntime = supportedRuntime,
      queueCapacity = config.queueCapacity,
      lastErrorCode = lastErrorCode
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
