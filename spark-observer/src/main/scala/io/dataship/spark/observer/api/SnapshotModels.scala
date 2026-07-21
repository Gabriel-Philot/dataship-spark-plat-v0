package io.dataship.spark.observer.api

import java.util.{ArrayList, LinkedHashMap}

final case class ApplicationView(
    appId: String,
    name: String,
    status: String,
    startedAt: String,
    completedAt: Option[String]
) {
  private[api] def allowlistedValues: java.util.Map[String, Object] = {
    val values = new LinkedHashMap[String, Object]()
    values.put("appId", appId)
    values.put("name", name)
    values.put("status", status)
    values.put("startedAt", startedAt)
    values.put("completedAt", completedAt.orNull)
    values
  }
}

final case class JobView(
    jobId: Int,
    status: String,
    startedAt: Option[String],
    completedAt: Option[String],
    stageIds: Seq[Int]
) {
  private[api] def allowlistedValues: java.util.Map[String, Object] = {
    val values = new LinkedHashMap[String, Object]()
    val stages = new ArrayList[Object]()
    stageIds.foreach(stageId => stages.add(Int.box(stageId)))
    values.put("jobId", Int.box(jobId))
    values.put("status", status)
    values.put("startedAt", startedAt.orNull)
    values.put("completedAt", completedAt.orNull)
    values.put("stageIds", stages)
    values
  }
}

final case class TaskAggregateView(
    total: Int,
    active: Int,
    completed: Int,
    failed: Int,
    killed: Int,
    completedIndices: Int
) {
  private[api] def allowlistedValues: java.util.Map[String, Object] = {
    val values = new LinkedHashMap[String, Object]()
    values.put("total", Int.box(total))
    values.put("active", Int.box(active))
    values.put("completed", Int.box(completed))
    values.put("failed", Int.box(failed))
    values.put("killed", Int.box(killed))
    values.put("completedIndices", Int.box(completedIndices))
    values
  }
}

final case class StageView(
    stageId: Int,
    attemptId: Int,
    status: String,
    startedAt: Option[String],
    completedAt: Option[String],
    tasks: TaskAggregateView
) {
  private[api] def allowlistedValues: java.util.Map[String, Object] = {
    val values = new LinkedHashMap[String, Object]()
    values.put("stageId", Int.box(stageId))
    values.put("attemptId", Int.box(attemptId))
    values.put("status", status)
    values.put("startedAt", startedAt.orNull)
    values.put("completedAt", completedAt.orNull)
    values.put("tasks", tasks.allowlistedValues)
    values
  }
}

final case class SqlExecutionView(id: Long, status: String)

final case class SnapshotResponse(
    schemaVersion: String,
    pluginVersion: String,
    sparkVersion: String,
    appId: String,
    mode: String,
    capturedAt: String,
    limit: Int,
    truncated: Boolean,
    application: ApplicationView,
    jobs: Seq[JobView],
    stages: Seq[StageView]
) {
  private[api] def allowlistedValues: java.util.Map[String, Object] = {
    val values = new LinkedHashMap[String, Object]()
    val jobValues = new ArrayList[Object]()
    val stageValues = new ArrayList[Object]()
    jobs.foreach(job => jobValues.add(job.allowlistedValues))
    stages.foreach(stage => stageValues.add(stage.allowlistedValues))
    values.put("schemaVersion", schemaVersion)
    values.put("pluginVersion", pluginVersion)
    values.put("sparkVersion", sparkVersion)
    values.put("appId", appId)
    values.put("mode", mode)
    values.put("capturedAt", capturedAt)
    values.put("limit", Int.box(limit))
    values.put("truncated", Boolean.box(truncated))
    values.put("application", application.allowlistedValues)
    values.put("jobs", jobValues)
    values.put("stages", stageValues)
    values
  }
}

private[api] final case class SnapshotErrorResponse(errorCode: String) {
  def allowlistedValues: java.util.Map[String, Object] = {
    val values = new LinkedHashMap[String, Object]()
    values.put("schemaVersion", "v1")
    values.put("errorCode", errorCode)
    values
  }
}
