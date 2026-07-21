package io.dataship.spark.observer.events

import java.time.{Clock, Instant}

import org.apache.spark.scheduler._

final case class ObserverListenerSnapshot(
    receivedByCategory: Map[String, Long],
    internalFailures: Long,
    lastEventAt: Option[Instant]
)

final class ObserverListener(
    queue: BoundedEventQueue,
    clock: Clock = Clock.systemUTC(),
    isSqlEvent: SparkListenerEvent => Boolean = ObserverListener.isSqlEvent
) extends SparkListener {
  override def onApplicationStart(event: SparkListenerApplicationStart): Unit =
    observe("application")

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit =
    observe("application")

  override def onJobStart(event: SparkListenerJobStart): Unit = observe("job")

  override def onJobEnd(event: SparkListenerJobEnd): Unit = observe("job")

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit =
    observe("stage")

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit =
    observe("stage")

  override def onTaskStart(event: SparkListenerTaskStart): Unit = observe("task")

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit =
    observe("task")

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = observe("task")

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit =
    observe("executor")

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit =
    observe("executor")

  override def onOtherEvent(event: SparkListenerEvent): Unit =
    observe(if (isSqlEvent(event)) "sql" else "other")

  def snapshot: ObserverListenerSnapshot = {
    val counters = queue.state.snapshot
    ObserverListenerSnapshot(
      receivedByCategory = counters.receivedByCategory,
      internalFailures = counters.internalFailures,
      lastEventAt = counters.lastEventAt
    )
  }

  private def observe(category: String): Unit = {
    queue.offer(ObserverEvent(category, Instant.now(clock)))
    ()
  }
}

object ObserverListener {
  private val SparkSqlUiEventPrefix =
    "org.apache.spark.sql.execution.ui.SparkListener"

  private[events] def isSqlEvent(event: SparkListenerEvent): Boolean =
    Option(event).exists(_.getClass.getName.startsWith(SparkSqlUiEventPrefix))
}
