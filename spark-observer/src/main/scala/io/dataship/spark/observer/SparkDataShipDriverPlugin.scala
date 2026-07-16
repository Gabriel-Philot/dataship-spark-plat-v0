package io.dataship.spark.observer

import java.util.Collections

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.apache.spark.dataship.v412.Spark412Bridge
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

trait HealthEndpointInstaller {
  def install(runtime: ObserverRuntime): Option[AutoCloseable]
}

final class SparkDataShipDriverPlugin private[observer] (
    installerFactory: SparkContext => HealthEndpointInstaller,
    runtimeFactory: (ObserverConfig, String) => ObserverRuntime
) extends DriverPlugin {
  private[observer] def this(
      installerFactory: SparkContext => HealthEndpointInstaller
  ) = this(installerFactory, SparkDataShipDriverPlugin.createRuntime)

  def this() = this(
    sparkContext => new Spark412Bridge(sparkContext),
    SparkDataShipDriverPlugin.createRuntime
  )

  private val logger = LoggerFactory.getLogger(classOf[SparkDataShipDriverPlugin])
  private var sparkContext: Option[SparkContext] = None
  private var runtime: Option[ObserverRuntime] = None
  private var endpointResource: Option[AutoCloseable] = None
  private var installationAttempted = false

  override def init(
      initializedSparkContext: SparkContext,
      pluginContext: PluginContext
  ): java.util.Map[String, String] = synchronized {
    val (config, initialErrorCode) = try {
      ObserverConfig.from(initializedSparkContext.getConf) -> ""
    } catch {
      case NonFatal(_) => ObserverConfig.Fallback -> "INVALID_CONFIG"
    }
    sparkContext = Some(initializedSparkContext)
    runtime = Some(runtimeFactory(config, initializedSparkContext.version))
    if (initialErrorCode.nonEmpty) {
      runtime.foreach(_.markFailure(initialErrorCode))
    }
    Collections.emptyMap[String, String]()
  }

  override def registerMetrics(
      appId: String,
      pluginContext: PluginContext
  ): Unit = synchronized {
    val hasValidApplicationId = runtime.exists(_.registerApplication(appId))
    if (hasValidApplicationId && !installationAttempted) {
      installationAttempted = true
      (sparkContext, runtime) match {
        case (Some(sc), Some(observerRuntime)) if observerRuntime.isSupportedRuntime =>
          installEndpoint(sc, observerRuntime)
        case (_, Some(_)) =>
          logger.warn(
            "DataShip Spark Observer did not install HTTP: " +
              "code=UNSUPPORTED_SPARK_VERSION"
          )
        case _ =>
          logger.warn(
            "DataShip Spark Observer did not install HTTP: " +
              "code=PLUGIN_NOT_INITIALIZED"
          )
      }
    }
  }

  override def shutdown(): Unit = synchronized {
    runtime.foreach(_.shutdown())
    endpointResource.foreach { resource =>
      try resource.close()
      catch {
        case error: LinkageError =>
          logger.warn(
            "DataShip Spark Observer could not detach HTTP: " +
              "code=HEALTH_DETACH_FAILED",
            error
          )
        case NonFatal(error) =>
          logger.warn(
            "DataShip Spark Observer could not detach HTTP: " +
              "code=HEALTH_DETACH_FAILED",
            error
          )
      }
    }
    endpointResource = None
  }

  private[observer] def currentHealthResponse =
    synchronized(runtime.map(_.healthResponse))

  private def installEndpoint(
      sc: SparkContext,
      observerRuntime: ObserverRuntime
  ): Unit = {
    try {
      endpointResource = installerFactory(sc).install(observerRuntime)
      endpointResource match {
        case Some(_) => observerRuntime.markUiAttached()
        case None =>
          observerRuntime.markNoSparkUi()
          logger.warn(
            "DataShip Spark Observer did not install HTTP: code=NO_SPARK_UI"
          )
      }
    } catch {
      case error: LinkageError =>
        observerRuntime.markFailure("HEALTH_INSTALL_FAILED")
        logger.warn(
          "DataShip Spark Observer degraded: code=HEALTH_INSTALL_FAILED",
          error
        )
      case NonFatal(error) =>
        observerRuntime.markFailure("HEALTH_INSTALL_FAILED")
        logger.warn(
          "DataShip Spark Observer degraded: code=HEALTH_INSTALL_FAILED",
          error
        )
    }
  }
}

private object SparkDataShipDriverPlugin {
  val createRuntime: (ObserverConfig, String) => ObserverRuntime =
    (config, sparkVersion) =>
      new ObserverRuntime(config = config, sparkVersion = sparkVersion)
}
