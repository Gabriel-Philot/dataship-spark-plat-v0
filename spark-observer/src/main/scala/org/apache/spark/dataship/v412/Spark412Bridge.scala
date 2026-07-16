package org.apache.spark.dataship.v412

import java.util.concurrent.atomic.AtomicBoolean

import io.dataship.spark.observer.{HealthEndpointInstaller, ObserverRuntime}
import io.dataship.spark.observer.api.HealthServlet
import org.apache.spark.SparkContext

final class Spark412Bridge(sparkContext: SparkContext)
    extends HealthEndpointInstaller {
  private val HealthPath = "/dataship/api/v1/health"

  override def install(runtime: ObserverRuntime): Option[AutoCloseable] = {
    sparkContext.ui.map { sparkUi =>
      val sparkUiClass = sparkUi.getClass
      val attachHandler = sparkUiClass.getMethod(
        "attachHandler",
        classOf[String],
        classOf[jakarta.servlet.http.HttpServlet],
        classOf[String]
      )
      val detachHandler = sparkUiClass.getMethod("detachHandler", classOf[String])
      attachHandler.invoke(sparkUi, HealthPath, new HealthServlet(runtime), "")
      new AutoCloseable {
        private val closed = new AtomicBoolean(false)

        override def close(): Unit = {
          if (closed.compareAndSet(false, true)) {
            detachHandler.invoke(sparkUi, HealthPath)
          }
        }
      }
    }
  }
}
