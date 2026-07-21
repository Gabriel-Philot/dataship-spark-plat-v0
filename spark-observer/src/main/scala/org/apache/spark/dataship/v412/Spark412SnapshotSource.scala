package org.apache.spark.dataship.v412

import java.time.Instant
import java.util.Date

import scala.jdk.CollectionConverters._

import io.dataship.spark.observer.api.{
  ApplicationView,
  JobView,
  SnapshotStoreNotReadyException,
  SparkSnapshotSource,
  SqlExecutionView,
  StageView,
  TaskAggregateView
}
import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.{
  AppStatusStore,
  JobDataWrapper,
  StageDataWrapper
}
import org.apache.spark.status.api.v1.{
  JobData,
  StageData,
  StageStatus
}

final class Spark412SnapshotSource(statusStore: AppStatusStore)
    extends SparkSnapshotSource {
  override def application(): ApplicationView = {
    try {
      val application = statusStore.applicationInfo()
      val attempt = application.attempts.headOption.getOrElse {
        throw new NoSuchElementException("application attempt is not ready")
      }
      val startedAt = timestamp(attempt.startTime).getOrElse {
        throw new NoSuchElementException("application start time is not ready")
      }
      ApplicationView(
        appId = application.id,
        name = application.name,
        status = if (attempt.completed) "COMPLETED" else "RUNNING",
        startedAt = startedAt,
        completedAt =
          if (attempt.completed) timestamp(attempt.endTime) else None
      )
    } catch {
      case _: NoSuchElementException =>
        throw new SnapshotStoreNotReadyException(
          "Spark application status is not ready."
        )
    }
  }

  override def jobs(limit: Int): Seq[JobView] =
    readBounded(classOf[JobDataWrapper], limit)(wrapper => mapJob(wrapper.info))

  override def stages(limit: Int): Seq[StageView] =
    readBounded(classOf[StageDataWrapper], limit)(wrapper => mapStage(wrapper.info))

  override def sqlExecutions(limit: Int): Seq[SqlExecutionView] = Vector.empty

  private def readBounded[Stored, View](
      storedClass: Class[Stored],
      limit: Int
  )(map: Stored => View): Vector[View] = {
    val iterator = statusStore.store
      .view(storedClass)
      .reverse()
      .max(limit.toLong + 1L)
      .closeableIterator()
    try iterator.asScala.map(map).toVector
    finally iterator.close()
  }

  private def mapJob(job: JobData): JobView = JobView(
    jobId = job.jobId,
    status = job.status match {
      case JobExecutionStatus.RUNNING => "RUNNING"
      case JobExecutionStatus.SUCCEEDED => "SUCCEEDED"
      case JobExecutionStatus.FAILED => "FAILED"
      case JobExecutionStatus.UNKNOWN => "UNKNOWN"
    },
    startedAt = job.submissionTime.flatMap(timestamp),
    completedAt = job.completionTime.flatMap(timestamp),
    stageIds = job.stageIds.map(_.asInstanceOf[Int]).toVector
  )

  private def mapStage(stage: StageData): StageView = {
    val totalTasks = nonNegative(stage.numTasks)
    StageView(
      stageId = stage.stageId,
      attemptId = stage.attemptId,
      status = stage.status match {
        case StageStatus.ACTIVE => "RUNNING"
        case StageStatus.COMPLETE => "SUCCEEDED"
        case StageStatus.FAILED => "FAILED"
        case StageStatus.PENDING => "PENDING"
        case StageStatus.SKIPPED => "SKIPPED"
      },
      startedAt = stage.submissionTime.flatMap(timestamp),
      completedAt = stage.completionTime.flatMap(timestamp),
      tasks = TaskAggregateView(
        total = totalTasks,
        active = nonNegative(stage.numActiveTasks),
        completed = nonNegative(stage.numCompleteTasks),
        failed = nonNegative(stage.numFailedTasks),
        killed = nonNegative(stage.numKilledTasks),
        completedIndices = math.min(
          totalTasks,
          nonNegative(stage.numCompletedIndices)
        )
      )
    )
  }

  private def timestamp(date: Date): Option[String] =
    Option(date).filter(_.getTime >= 0L).map(value => Instant.ofEpochMilli(value.getTime).toString)

  private def nonNegative(value: Int): Int = math.max(0, value)
}
