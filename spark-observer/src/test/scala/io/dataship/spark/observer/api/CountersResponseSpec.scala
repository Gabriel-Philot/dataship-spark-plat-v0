package io.dataship.spark.observer.api

import java.time.{Clock, Instant, ZoneOffset}
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import io.dataship.spark.observer.{ObserverConfig, ObserverRuntime}
import org.scalatest.funsuite.AnyFunSuite

final class CountersResponseSpec extends AnyFunSuite {
  private val fixedInstant = Instant.parse("2026-07-21T10:15:30Z")
  private val fixedClock = Clock.fixed(fixedInstant, ZoneOffset.UTC)

  test("renders the stable live counters envelope from one atomic queue snapshot") {
    val runtime = new ObserverRuntime(
      config = ObserverConfig(
        enabled = true,
        queueCapacity = 4,
        transitionsCapacity = 4
      ),
      sparkVersion = "4.1.2",
      clock = fixedClock
    )
    runtime.registerApplication("app-counters-test")
    val listener = runtime.listenerForInstallation.getOrElse(
      fail("An enabled runtime must provide its listener for installation")
    )
    runtime.markListenerInstalled()

    try {
      listener.onJobStart(null)
      awaitCondition("the listener event to finish processing") {
        runtime.countersResponse.processed == 1L
      }

      val json = JsonRenderer.render(runtime.countersResponse)
      val document = new ObjectMapper().readTree(json)
      assert(document.fieldNames().asScala.toSet == Set(
        "schemaVersion",
        "pluginVersion",
        "sparkVersion",
        "appId",
        "mode",
        "capturedAt",
        "receivedByCategory",
        "listenerReceived",
        "processed",
        "queued",
        "inFlight",
        "droppedByPlugin",
        "internalFailures",
        "depth",
        "capacity",
        "lastEventAt",
        "invariantHolds"
      ))
      assert(document.get("schemaVersion").asText() == "v1")
      assert(document.get("pluginVersion").asText() == "0.1.0-SNAPSHOT")
      assert(document.get("sparkVersion").asText() == "4.1.2")
      assert(document.get("appId").asText() == "app-counters-test")
      assert(document.get("mode").asText() == "live")
      assert(document.get("capturedAt").asText() == fixedInstant.toString)
      assert(document.get("listenerReceived").asLong() == 1L)
      assert(document.get("processed").asLong() == 1L)
      assert(document.get("queued").asLong() == 0L)
      assert(document.get("inFlight").asLong() == 0L)
      assert(document.get("droppedByPlugin").asLong() == 0L)
      assert(document.get("internalFailures").asLong() == 0L)
      assert(document.get("depth").asLong() == 0L)
      assert(document.get("capacity").asInt() == 4)
      assert(document.get("lastEventAt").asText() == fixedInstant.toString)
      assert(document.get("invariantHolds").asBoolean())

      val categories = document.get("receivedByCategory")
      assert(categories.fieldNames().asScala.toSet == Set(
        "application",
        "job",
        "stage",
        "task",
        "sql",
        "executor",
        "other"
      ))
      assert(categories.get("job").asLong() == 1L)
      assert(
        categories.elements().asScala.map(_.asLong()).sum ==
          document.get("listenerReceived").asLong()
      )
      assert(runtime.healthResponse.listenerInstalled)
    } finally {
      runtime.shutdown()
    }
  }

  private def awaitCondition(description: String)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    while (!condition && System.nanoTime() < deadline) {
      Thread.sleep(5L)
    }
    assert(condition, s"Timed out waiting for $description")
  }
}
