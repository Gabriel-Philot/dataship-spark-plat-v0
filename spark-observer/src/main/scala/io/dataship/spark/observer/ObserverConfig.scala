package io.dataship.spark.observer

import org.apache.spark.SparkConf

final case class ObserverConfig(
    enabled: Boolean,
    queueCapacity: Int,
    transitionsCapacity: Int = 128,
    testMode: Boolean = false,
    testProcessingDelayMs: Int = 0,
    snapshotLimit: Int = 50
)

object ObserverConfig {
  private val EnabledKey = "spark.dataship.observer.enabled"
  private val QueueCapacityKey = "spark.dataship.observer.queue.capacity"
  private val TransitionsCapacityKey =
    "spark.dataship.observer.transitions.capacity"
  private val TestModeKey = "spark.dataship.observer.testMode"
  private val TestProcessingDelayKey =
    "spark.dataship.observer.test.processingDelayMs"
  private val SnapshotLimitKey = "spark.dataship.observer.snapshot.limit"
  private val DefaultQueueCapacity = 1024
  private val DefaultTransitionsCapacity = 128
  private val MinimumQueueCapacity = 1
  private val MaximumQueueCapacity = 65536
  private val MinimumTransitionsCapacity = 1
  private val MaximumTransitionsCapacity = 1024
  private val MaximumTestProcessingDelayMs = 1000
  private val DefaultSnapshotLimit = 50
  private val MinimumSnapshotLimit = 1
  private val MaximumSnapshotLimit = 200

  private[observer] val Fallback: ObserverConfig =
    ObserverConfig(
      enabled = false,
      queueCapacity = DefaultQueueCapacity,
      transitionsCapacity = DefaultTransitionsCapacity,
      testMode = false,
      testProcessingDelayMs = 0,
      snapshotLimit = DefaultSnapshotLimit
    )

  def from(sparkConf: SparkConf): ObserverConfig = {
    val queueCapacity = try {
      sparkConf.getInt(QueueCapacityKey, DefaultQueueCapacity)
    } catch {
      case error: NumberFormatException =>
        throw invalidQueueCapacity(sparkConf.get(QueueCapacityKey), error)
    }
    if (queueCapacity < MinimumQueueCapacity || queueCapacity > MaximumQueueCapacity) {
      throw invalidQueueCapacity(queueCapacity.toString)
    }
    val transitionsCapacity = try {
      sparkConf.getInt(TransitionsCapacityKey, DefaultTransitionsCapacity)
    } catch {
      case error: NumberFormatException =>
        throw invalidTransitionsCapacity(
          sparkConf.get(TransitionsCapacityKey),
          error
        )
    }
    if (
      transitionsCapacity < MinimumTransitionsCapacity ||
      transitionsCapacity > MaximumTransitionsCapacity
    ) {
      throw invalidTransitionsCapacity(transitionsCapacity.toString)
    }
    val testMode = sparkConf.getBoolean(TestModeKey, defaultValue = false)
    val testProcessingDelayMs = try {
      sparkConf.getInt(TestProcessingDelayKey, 0)
    } catch {
      case error: NumberFormatException =>
        throw invalidTestProcessingDelay(
          sparkConf.get(TestProcessingDelayKey),
          error
        )
    }
    if (
      testProcessingDelayMs < 0 ||
      testProcessingDelayMs > MaximumTestProcessingDelayMs
    ) {
      throw invalidTestProcessingDelay(testProcessingDelayMs.toString)
    }
    if (!testMode && testProcessingDelayMs != 0) {
      throw new IllegalArgumentException(
        s"$TestProcessingDelayKey requires testMode=true when non-zero."
      )
    }
    val snapshotLimit = try {
      sparkConf.getInt(SnapshotLimitKey, DefaultSnapshotLimit)
    } catch {
      case error: NumberFormatException =>
        throw invalidSnapshotLimit(sparkConf.get(SnapshotLimitKey), error)
    }
    if (
      snapshotLimit < MinimumSnapshotLimit ||
      snapshotLimit > MaximumSnapshotLimit
    ) {
      throw invalidSnapshotLimit(snapshotLimit.toString)
    }

    ObserverConfig(
      enabled = sparkConf.getBoolean(EnabledKey, defaultValue = false),
      queueCapacity = queueCapacity,
      transitionsCapacity = transitionsCapacity,
      testMode = testMode,
      testProcessingDelayMs = testProcessingDelayMs,
      snapshotLimit = snapshotLimit
    )
  }

  private def invalidQueueCapacity(
      value: String,
      cause: Throwable = null
  ): IllegalArgumentException =
    new IllegalArgumentException(
      s"$QueueCapacityKey must be an integer from $MinimumQueueCapacity to " +
        s"$MaximumQueueCapacity, but was '$value'.",
      cause
    )

  private def invalidTransitionsCapacity(
      value: String,
      cause: Throwable = null
  ): IllegalArgumentException =
    new IllegalArgumentException(
      s"$TransitionsCapacityKey must be an integer from " +
        s"$MinimumTransitionsCapacity to $MaximumTransitionsCapacity, " +
        s"but was '$value'.",
      cause
    )

  private def invalidTestProcessingDelay(
      value: String,
      cause: Throwable = null
  ): IllegalArgumentException =
    new IllegalArgumentException(
      s"$TestProcessingDelayKey must be an integer from 0 to " +
        s"$MaximumTestProcessingDelayMs, but was '$value'.",
      cause
    )

  private def invalidSnapshotLimit(
      value: String,
      cause: Throwable = null
  ): IllegalArgumentException =
    new IllegalArgumentException(
      s"$SnapshotLimitKey must be an integer from $MinimumSnapshotLimit to " +
        s"$MaximumSnapshotLimit, but was '$value'.",
      cause
    )
}
