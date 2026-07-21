package io.dataship.spark.observer.api

import java.time.{Clock, Instant, ZoneOffset}

import io.dataship.spark.observer.{ObserverConfig, ObserverRuntime}
import org.scalatest.funsuite.AnyFunSuite

final class LiveSnapshotServiceSpec extends AnyFunSuite {
  private val fixedInstant = Instant.parse("2026-07-21T15:00:00Z")
  private val fixedClock = Clock.fixed(fixedInstant, ZoneOffset.UTC)

  test("uses the configured default limit and trims the source sentinel") {
    val source = new RecordingSnapshotSource(
      jobsToReturn = Vector(job(3), job(2), job(1)),
      stagesToReturn = Vector(stage(3), stage(2), stage(1))
    )
    val service = new LiveSnapshotService(
      runtime(snapshotLimit = 2),
      source
    )

    val response = service.snapshot(requestedLimit = None)

    assert(response.limit == 2)
    assert(response.jobs.map(_.jobId) == Vector(3, 2))
    assert(response.stages.map(_.stageId) == Vector(3, 2))
    assert(response.truncated)
    assert(source.requestedJobLimits == Vector(2))
    assert(source.requestedStageLimits == Vector(2))
  }

  test("accepts explicit limits at both boundaries without reordering source data") {
    Seq(1, 200).foreach { requestedLimit =>
      val source = new RecordingSnapshotSource(
        jobsToReturn = Vector(job(9), job(8)),
        stagesToReturn = Vector(stage(9), stage(8))
      )
      val service = new LiveSnapshotService(runtime(), source)

      val response = service.snapshot(Some(requestedLimit))

      assert(response.limit == requestedLimit)
      assert(response.jobs.map(_.jobId) == Vector(9, 8).take(requestedLimit))
      assert(
        response.stages.map(_.stageId) == Vector(9, 8).take(requestedLimit)
      )
      assert(source.requestedJobLimits == Vector(requestedLimit))
      assert(source.requestedStageLimits == Vector(requestedLimit))
      assert(response.truncated == (requestedLimit == 1))
    }
  }

  test("rejects limits outside the stable HTTP contract") {
    val service = new LiveSnapshotService(runtime(), new RecordingSnapshotSource())

    Seq(0, 201).foreach { invalidLimit =>
      val error = intercept[InvalidSnapshotLimitException] {
        service.snapshot(Some(invalidLimit))
      }
      assert(error.getMessage.contains("1 to 200"))
    }
  }

  test("renders only Observer DTOs and stable allowlisted snapshot fields") {
    val source = new RecordingSnapshotSource(
      jobsToReturn = Vector(job(7)),
      stagesToReturn = Vector(stage(7))
    )
    val response = new LiveSnapshotService(runtime(), source).snapshot(Some(1))

    val document = JsonRenderer.objectMapper.readTree(JsonRenderer.render(response))

    assert(document.fieldNames().next() == "schemaVersion")
    assert(document.get("schemaVersion").asText() == "v1")
    assert(document.get("pluginVersion").asText() == "0.1.0-SNAPSHOT")
    assert(document.get("sparkVersion").asText() == "4.1.2")
    assert(document.get("appId").asText() == "app-snapshot-test")
    assert(document.get("mode").asText() == "live")
    assert(document.get("capturedAt").asText() == fixedInstant.toString)
    assert(document.get("limit").asInt() == 1)
    assert(document.get("application").get("status").asText() == "RUNNING")
    assert(document.get("jobs").get(0).get("jobId").asInt() == 7)
    assert(document.get("stages").get(0).get("stageId").asInt() == 7)
    assert(document.get("stages").get(0).get("tasks").get("total").asInt() == 4)
    assert(!JsonRenderer.render(response).contains("sparkConf"))
    assert(!JsonRenderer.render(response).contains("details"))
    assert(!JsonRenderer.render(response).contains("tasksById"))
  }

  private def runtime(snapshotLimit: Int = 50): ObserverRuntime = {
    val observerRuntime = new ObserverRuntime(
      config = ObserverConfig(
        enabled = true,
        queueCapacity = 1024,
        snapshotLimit = snapshotLimit
      ),
      sparkVersion = "4.1.2",
      clock = fixedClock
    )
    observerRuntime.registerApplication("app-snapshot-test")
    observerRuntime
  }

  private def job(id: Int): JobView = JobView(
    jobId = id,
    status = if (id == 3) "RUNNING" else "SUCCEEDED",
    startedAt = Some("2026-07-21T14:59:00Z"),
    completedAt = if (id == 3) None else Some("2026-07-21T14:59:30Z"),
    stageIds = Vector(id)
  )

  private def stage(id: Int): StageView = StageView(
    stageId = id,
    attemptId = 0,
    status = if (id == 3) "RUNNING" else "SUCCEEDED",
    startedAt = Some("2026-07-21T14:59:00Z"),
    completedAt = if (id == 3) None else Some("2026-07-21T14:59:30Z"),
    tasks = TaskAggregateView(
      total = 4,
      active = if (id == 3) 1 else 0,
      completed = if (id == 3) 3 else 4,
      failed = 0,
      killed = 0,
      completedIndices = if (id == 3) 3 else 4
    )
  )

  private final class RecordingSnapshotSource(
      jobsToReturn: Seq[JobView] = Vector.empty,
      stagesToReturn: Seq[StageView] = Vector.empty
  ) extends SparkSnapshotSource {
    var requestedJobLimits = Vector.empty[Int]
    var requestedStageLimits = Vector.empty[Int]

    override def application(): ApplicationView = ApplicationView(
      appId = "app-snapshot-test",
      name = "snapshot-test",
      status = "RUNNING",
      startedAt = "2026-07-21T14:58:00Z",
      completedAt = None
    )

    override def jobs(limit: Int): Seq[JobView] = {
      requestedJobLimits :+= limit
      jobsToReturn.take(limit + 1)
    }

    override def stages(limit: Int): Seq[StageView] = {
      requestedStageLimits :+= limit
      stagesToReturn.take(limit + 1)
    }

    override def sqlExecutions(limit: Int): Seq[SqlExecutionView] = Vector.empty
  }
}
