package io.dataship.spark.observer.api

import java.time.{Clock, Instant, ZoneOffset}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import io.dataship.spark.observer.{
  HealthEndpointInstaller,
  ObserverConfig,
  ObserverRuntime,
  SparkDataShipDriverPlugin
}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite

final class HealthResponseSpec extends AnyFunSuite {
  private val fixedClock =
    Clock.fixed(Instant.parse("2026-07-16T12:34:56Z"), ZoneOffset.UTC)

  test("renders only the stable health envelope and allowlisted fields") {
    val runtime = new ObserverRuntime(
      ObserverConfig(enabled = true, queueCapacity = 64),
      sparkVersion = "4.1.2",
      clock = fixedClock
    )
    runtime.registerApplication("app-health-test")
    runtime.markUiAttached()

    val json = JsonRenderer.render(runtime.healthResponse)
    val document = new ObjectMapper().readTree(json)

    assert(document.fieldNames().asScala.toSet == Set(
      "schemaVersion",
      "pluginVersion",
      "sparkVersion",
      "appId",
      "mode",
      "capturedAt",
      "status",
      "uiAttached",
      "listenerInstalled",
      "supportedRuntime",
      "queueCapacity",
      "lastErrorCode"
    ))
    assert(document.get("schemaVersion").asText() == "v1")
    assert(document.get("pluginVersion").asText() == "0.1.0-SNAPSHOT")
    assert(document.get("sparkVersion").asText() == "4.1.2")
    assert(document.get("appId").asText() == "app-health-test")
    assert(document.get("mode").asText() == "live")
    assert(document.get("capturedAt").asText() == "2026-07-16T12:34:56Z")
    assert(document.get("status").asText() == "READY")
    assert(document.get("uiAttached").asBoolean())
    assert(!document.get("listenerInstalled").asBoolean())
    assert(document.get("supportedRuntime").asBoolean())
    assert(document.get("queueCapacity").asInt() == 64)
    assert(document.get("lastErrorCode").asText() == "")
  }

  test("moves through starting, ready, and stopping without installing a listener") {
    val runtime = new ObserverRuntime(
      ObserverConfig(enabled = true, queueCapacity = 1024),
      sparkVersion = "4.1.2",
      clock = fixedClock
    )

    assert(runtime.healthResponse.status == "STARTING")
    runtime.registerApplication("app-lifecycle-test")
    runtime.markUiAttached()
    assert(runtime.healthResponse.status == "READY")
    assert(!runtime.healthResponse.listenerInstalled)

    runtime.shutdown()
    runtime.shutdown()
    assert(runtime.healthResponse.status == "STOPPING")
  }

  test("keeps disabled mode explicit when the health endpoint is attached") {
    val runtime = new ObserverRuntime(
      ObserverConfig(enabled = false, queueCapacity = 1024),
      sparkVersion = "4.1.2",
      clock = fixedClock
    )

    runtime.registerApplication("app-disabled-test")
    runtime.markUiAttached()

    assert(runtime.healthResponse.status == "DISABLED")
    assert(runtime.healthResponse.uiAttached)
    assert(!runtime.healthResponse.listenerInstalled)
  }

  test("marks a supported enabled runtime as degraded when Spark has no UI") {
    val runtime = new ObserverRuntime(
      ObserverConfig(enabled = true, queueCapacity = 1024),
      sparkVersion = "4.1.2",
      clock = fixedClock
    )

    runtime.registerApplication("app-no-ui-test")
    runtime.markNoSparkUi()

    assert(runtime.healthResponse.status == "DEGRADED")
    assert(!runtime.healthResponse.uiAttached)
    assert(runtime.healthResponse.lastErrorCode == "NO_SPARK_UI")
  }

  test("does not claim support for a different Spark runtime") {
    val runtime = new ObserverRuntime(
      ObserverConfig(enabled = true, queueCapacity = 1024),
      sparkVersion = "4.1.1",
      clock = fixedClock
    )

    assert(!runtime.healthResponse.supportedRuntime)
    assert(runtime.healthResponse.status == "DEGRADED")
    assert(runtime.healthResponse.lastErrorCode == "UNSUPPORTED_SPARK_VERSION")
  }

  test("stores SparkContext during init and installs the health endpoint at most once") {
    withSparkContext(uiEnabled = false) { sparkContext =>
      val factoryCalls = new AtomicInteger(0)
      val installCalls = new AtomicInteger(0)
      val closeCalls = new AtomicInteger(0)
      var observedSparkContext: SparkContext = null
      val installer = new HealthEndpointInstaller {
        override def install(runtime: ObserverRuntime): Option[AutoCloseable] = {
          installCalls.incrementAndGet()
          Some(new AutoCloseable {
            override def close(): Unit = closeCalls.incrementAndGet()
          })
        }
      }
      val plugin = new SparkDataShipDriverPlugin(sc => {
        factoryCalls.incrementAndGet()
        observedSparkContext = sc
        installer
      })

      plugin.init(sparkContext, null)
      assert(factoryCalls.get() == 0)

      plugin.registerMetrics("app-install-once", null)
      plugin.registerMetrics("app-install-once", null)

      assert(observedSparkContext eq sparkContext)
      assert(factoryCalls.get() == 1)
      assert(installCalls.get() == 1)
      assert(plugin.currentHealthResponse.exists(_.status == "READY"))

      plugin.shutdown()
      plugin.shutdown()
      assert(closeCalls.get() == 1)
      assert(plugin.currentHealthResponse.exists(_.status == "STOPPING"))
    }
  }

  test("waits for a valid application ID before consuming the installation attempt") {
    withSparkContext(uiEnabled = false) { sparkContext =>
      val factoryCalls = new AtomicInteger(0)
      val installCalls = new AtomicInteger(0)
      val installer = new HealthEndpointInstaller {
        override def install(runtime: ObserverRuntime): Option[AutoCloseable] = {
          installCalls.incrementAndGet()
          Some(() => ())
        }
      }
      val plugin = new SparkDataShipDriverPlugin(_ => {
        factoryCalls.incrementAndGet()
        installer
      })

      plugin.init(sparkContext, null)
      plugin.registerMetrics("   ", null)
      plugin.registerMetrics(null, null)

      assert(factoryCalls.get() == 0)
      assert(installCalls.get() == 0)
      assert(plugin.currentHealthResponse.exists(_.lastErrorCode == "EMPTY_APP_ID"))

      plugin.registerMetrics("app-recovered-after-empty-id", null)
      plugin.registerMetrics("app-recovered-after-empty-id", null)

      assert(factoryCalls.get() == 1)
      assert(installCalls.get() == 1)
      val health = plugin.currentHealthResponse.getOrElse(
        fail("The driver plugin did not retain its health runtime")
      )
      assert(health.appId == "app-recovered-after-empty-id")
      assert(health.status == "READY")
      assert(health.lastErrorCode.isEmpty)

      plugin.shutdown()
    }
  }

  test("preserves an unrelated failure across invalid and valid application IDs") {
    val runtime = new ObserverRuntime(
      ObserverConfig(enabled = true, queueCapacity = 1024),
      sparkVersion = "4.1.2",
      clock = fixedClock
    )

    runtime.markFailure("INVALID_CONFIG")
    assert(!runtime.registerApplication(" "))
    assert(!runtime.registerApplication(null))
    assert(runtime.registerApplication("app-with-unrelated-failure"))
    runtime.markUiAttached()

    val health = runtime.healthResponse
    assert(health.appId == "app-with-unrelated-failure")
    assert(health.status == "DEGRADED")
    assert(health.lastErrorCode == "INVALID_CONFIG")
  }

  test("ignores invalid application IDs after reaching ready") {
    val runtime = new ObserverRuntime(
      ObserverConfig(enabled = true, queueCapacity = 1024),
      sparkVersion = "4.1.2",
      clock = fixedClock
    )

    assert(runtime.registerApplication("app-already-ready"))
    runtime.markUiAttached()
    assert(!runtime.registerApplication(" "))
    assert(!runtime.registerApplication(null))

    val health = runtime.healthResponse
    assert(health.appId == "app-already-ready")
    assert(health.status == "READY")
    assert(health.lastErrorCode.isEmpty)
  }

  test("fails open when the installer factory throws and shutdown remains safe") {
    withSparkContext(uiEnabled = false) { sparkContext =>
      val factoryCalls = new AtomicInteger(0)
      val plugin = new SparkDataShipDriverPlugin(_ => {
        factoryCalls.incrementAndGet()
        throw new IllegalStateException("expected installer factory failure")
      })

      plugin.init(sparkContext, null)
      plugin.registerMetrics("app-install-failure", null)

      assert(factoryCalls.get() == 1)
      val health = plugin.currentHealthResponse.getOrElse(
        fail("The driver plugin did not retain its health runtime")
      )
      assert(health.appId == "app-install-failure")
      assert(health.status == "DEGRADED")
      assert(health.lastErrorCode == "HEALTH_INSTALL_FAILED")

      plugin.shutdown()
      plugin.shutdown()
      assert(plugin.currentHealthResponse.exists(_.status == "STOPPING"))
    }
  }

  test("does not instantiate the installer factory for an unsupported runtime") {
    withSparkContext(uiEnabled = false) { sparkContext =>
      val factoryCalls = new AtomicInteger(0)
      val plugin = new SparkDataShipDriverPlugin(
        _ => {
          factoryCalls.incrementAndGet()
          fail("The installer factory must not run for an unsupported runtime")
        },
        (config, _) =>
          new ObserverRuntime(
            config = config,
            sparkVersion = "4.1.1",
            clock = fixedClock
          )
      )

      plugin.init(sparkContext, null)
      plugin.registerMetrics("app-unsupported-runtime", null)

      assert(factoryCalls.get() == 0)
      val health = plugin.currentHealthResponse.getOrElse(
        fail("The driver plugin did not retain its health runtime")
      )
      assert(!health.supportedRuntime)
      assert(!health.uiAttached)
      assert(health.status == "DEGRADED")
      assert(health.lastErrorCode == "UNSUPPORTED_SPARK_VERSION")

      plugin.shutdown()
    }
  }

  test("uses the SparkContext stored by init and degrades cleanly when sc.ui is empty") {
    withSparkContext(uiEnabled = false) { sparkContext =>
      val plugin = new SparkDataShipDriverPlugin

      plugin.init(sparkContext, null)
      plugin.registerMetrics("app-no-spark-ui", null)

      val health = plugin.currentHealthResponse.getOrElse(
        fail("The driver plugin did not retain its health runtime")
      )
      assert(health.status == "DEGRADED")
      assert(!health.uiAttached)
      assert(health.lastErrorCode == "NO_SPARK_UI")
      assert(!health.listenerInstalled)

      plugin.shutdown()
      plugin.shutdown()
    }
  }

  private def withSparkContext(uiEnabled: Boolean)(testBody: SparkContext => Unit): Unit = {
    val sparkConf = new SparkConf(false)
      .setMaster("local[1]")
      .setAppName(s"dataship-health-spec-${System.nanoTime()}")
      .set("spark.ui.enabled", uiEnabled.toString)
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.dataship.observer.enabled", "true")
    val sparkContext = new SparkContext(sparkConf)
    try testBody(sparkContext)
    finally sparkContext.stop()
  }
}
