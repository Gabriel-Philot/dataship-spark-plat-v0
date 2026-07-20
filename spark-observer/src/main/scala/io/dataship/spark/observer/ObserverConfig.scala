package io.dataship.spark.observer

import org.apache.spark.SparkConf

final case class ObserverConfig(
    enabled: Boolean,
    queueCapacity: Int,
    transitionsCapacity: Int = 128
)

object ObserverConfig {
  private val EnabledKey = "spark.dataship.observer.enabled"
  private val QueueCapacityKey = "spark.dataship.observer.queue.capacity"
  private val TransitionsCapacityKey =
    "spark.dataship.observer.transitions.capacity"
  private val DefaultQueueCapacity = 1024
  private val DefaultTransitionsCapacity = 128
  private val MinimumQueueCapacity = 1
  private val MaximumQueueCapacity = 65536
  private val MinimumTransitionsCapacity = 1
  private val MaximumTransitionsCapacity = 1024

  private[observer] val Fallback: ObserverConfig =
    ObserverConfig(
      enabled = false,
      queueCapacity = DefaultQueueCapacity,
      transitionsCapacity = DefaultTransitionsCapacity
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

    ObserverConfig(
      enabled = sparkConf.getBoolean(EnabledKey, defaultValue = false),
      queueCapacity = queueCapacity,
      transitionsCapacity = transitionsCapacity
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
}
