package io.dataship.spark.observer.api

import io.dataship.spark.observer.ObserverRuntime

final class InvalidSnapshotLimitException(message: String)
    extends IllegalArgumentException(message)

final class LiveSnapshotService(
    runtime: ObserverRuntime,
    source: SparkSnapshotSource
) {
  def snapshot(requestedLimit: Option[Int]): SnapshotResponse = {
    val limit = requestedLimit.getOrElse(runtime.config.snapshotLimit)
    if (limit < LiveSnapshotService.MinimumLimit || limit > LiveSnapshotService.MaximumLimit) {
      throw new InvalidSnapshotLimitException(
        s"Snapshot limit must be an integer from ${LiveSnapshotService.MinimumLimit} " +
          s"to ${LiveSnapshotService.MaximumLimit}."
      )
    }

    val application = source.application()
    val jobCandidates = source.jobs(limit)
    val stageCandidates = source.stages(limit)
    val health = runtime.healthResponse

    SnapshotResponse(
      schemaVersion = health.schemaVersion,
      pluginVersion = health.pluginVersion,
      sparkVersion = health.sparkVersion,
      appId = health.appId,
      mode = health.mode,
      capturedAt = health.capturedAt,
      limit = limit,
      truncated = jobCandidates.size > limit || stageCandidates.size > limit,
      application = application,
      jobs = jobCandidates.take(limit),
      stages = stageCandidates.take(limit)
    )
  }
}

private object LiveSnapshotService {
  val MinimumLimit = 1
  val MaximumLimit = 200
}
