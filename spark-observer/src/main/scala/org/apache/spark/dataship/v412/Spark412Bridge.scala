package org.apache.spark.dataship.v412

import java.util.concurrent.atomic.AtomicBoolean

import io.dataship.spark.observer.{HealthEndpointInstaller, ObserverRuntime}
import io.dataship.spark.observer.api.{
  CountersServlet,
  HealthServlet,
  LiveSnapshotService,
  SnapshotServlet
}
import org.apache.spark.SparkContext

final class Spark412Bridge(sparkContext: SparkContext)
    extends HealthEndpointInstaller {
  private val HealthPath = "/dataship/api/v1/health"
  private val CountersPath = "/dataship/api/v1/debug/counters"
  private val SnapshotPath = "/dataship/api/v1/snapshot"

  override def install(runtime: ObserverRuntime): Option[AutoCloseable] = {
    sparkContext.ui.map { sparkUi =>
      val listener = runtime.listenerForInstallation
      val sparkUiClass = sparkUi.getClass
      val attachHandler = sparkUiClass.getMethod(
        "attachHandler",
        classOf[String],
        classOf[jakarta.servlet.http.HttpServlet],
        classOf[String]
      )
      val detachHandler = sparkUiClass.getMethod("detachHandler", classOf[String])
      attachHandler.invoke(sparkUi, HealthPath, new HealthServlet(runtime), "")
      try {
        attachHandler.invoke(
          sparkUi,
          CountersPath,
          new CountersServlet(runtime),
          ""
        )
        if (runtime.config.enabled) {
          attachHandler.invoke(
            sparkUi,
            SnapshotPath,
            new SnapshotServlet(
              new LiveSnapshotService(
                runtime,
                new Spark412SnapshotSource(sparkContext.statusStore)
              )
            ),
            ""
          )
        }
        listener.foreach { observerListener =>
          sparkContext.listenerBus.addToQueue(
            observerListener,
            Spark412Bridge.ListenerQueueName
          )
          runtime.markListenerInstalled()
        }
      } catch {
        case error: Throwable =>
          if (runtime.config.enabled) {
            detachHandler.invoke(sparkUi, SnapshotPath)
          }
          detachHandler.invoke(sparkUi, CountersPath)
          detachHandler.invoke(sparkUi, HealthPath)
          throw error
      }
      new AutoCloseable {
        private val closed = new AtomicBoolean(false)

        override def close(): Unit = {
          if (closed.compareAndSet(false, true)) {
            if (runtime.config.enabled) {
              detachHandler.invoke(sparkUi, SnapshotPath)
            }
            detachHandler.invoke(sparkUi, CountersPath)
            detachHandler.invoke(sparkUi, HealthPath)
            listener.foreach(sparkContext.removeSparkListener)
          }
        }
      }
    }
  }
}

object Spark412Bridge {
  val ListenerQueueName = "dataship-observer"
}
