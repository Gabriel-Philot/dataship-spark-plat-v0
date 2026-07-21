package io.dataship.spark.observer.api

import java.time.{Clock, Instant, ZoneOffset}

import com.fasterxml.jackson.databind.ObjectMapper
import io.dataship.spark.observer.{ObserverConfig, ObserverRuntime}
import org.scalatest.funsuite.AnyFunSuite

final class SnapshotServletSpec extends AnyFunSuite {
  private val mapper = new ObjectMapper()

  test("returns 200 JSON for the configured default and an explicit limit") {
    val servlet = new SnapshotServlet(service(new EmptySource))

    val defaultResult = servlet.responseFor(None)
    val explicitResult = servlet.responseFor(Some("1"))

    assert(defaultResult.statusCode == 200)
    assert(mapper.readTree(defaultResult.body).get("limit").asInt() == 50)
    assert(explicitResult.statusCode == 200)
    assert(mapper.readTree(explicitResult.body).get("limit").asInt() == 1)
  }

  test("returns 400 for empty, non-numeric, overflow, and out-of-range limits") {
    val servlet = new SnapshotServlet(service(new EmptySource))

    Seq("", "not-a-number", "999999999999999999999", "0", "201").foreach {
      rawLimit =>
        val result = servlet.responseFor(Some(rawLimit))
        assert(result.statusCode == 400)
        assert(mapper.readTree(result.body).get("errorCode").asText() == "INVALID_LIMIT")
    }
  }

  test("returns 409 only for a typed temporary store-not-ready failure") {
    val source = new EmptySource {
      override def application(): ApplicationView =
        throw new SnapshotStoreNotReadyException("store is starting")
    }

    val result = new SnapshotServlet(service(source)).responseFor(None)

    assert(result.statusCode == 409)
    assert(mapper.readTree(result.body).get("errorCode").asText() == "STORE_NOT_READY")
  }

  test("isolates unexpected failures as 500 without exposing their message") {
    val secretSentinel = "must-not-leak-snapshot-secret"
    val source = new EmptySource {
      override def application(): ApplicationView =
        throw new IllegalStateException(secretSentinel)
    }

    val result = new SnapshotServlet(service(source)).responseFor(None)

    assert(result.statusCode == 500)
    assert(mapper.readTree(result.body).get("errorCode").asText() == "SNAPSHOT_FAILED")
    assert(!result.body.contains(secretSentinel))
    assert(!result.body.contains("IllegalStateException"))
  }

  private def service(source: SparkSnapshotSource): LiveSnapshotService = {
    val runtime = new ObserverRuntime(
      config = ObserverConfig(enabled = true, queueCapacity = 1024),
      sparkVersion = "4.1.2",
      clock = Clock.fixed(
        Instant.parse("2026-07-21T15:00:00Z"),
        ZoneOffset.UTC
      )
    )
    runtime.registerApplication("app-snapshot-servlet-test")
    new LiveSnapshotService(runtime, source)
  }

  private class EmptySource extends SparkSnapshotSource {
    override def application(): ApplicationView = ApplicationView(
      appId = "app-snapshot-servlet-test",
      name = "snapshot-servlet-test",
      status = "RUNNING",
      startedAt = "2026-07-21T14:58:00Z",
      completedAt = None
    )
    override def jobs(limit: Int): Seq[JobView] = Vector.empty
    override def stages(limit: Int): Seq[StageView] = Vector.empty
    override def sqlExecutions(limit: Int): Seq[SqlExecutionView] = Vector.empty
  }
}
