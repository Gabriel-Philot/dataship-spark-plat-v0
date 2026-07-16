package io.dataship.spark.observer

import org.apache.spark.SparkConf

final case class ObserverConfig(enabled: Boolean, queueCapacity: Int)

object ObserverConfig {
  private val EnabledKey = "spark.dataship.observer.enabled"
  private val QueueCapacityKey = "spark.dataship.observer.queue.capacity"
  private val DefaultQueueCapacity = 1024
  private val MinimumQueueCapacity = 1
  private val MaximumQueueCapacity = 65536

  private[observer] val Fallback: ObserverConfig =
    ObserverConfig(enabled = false, queueCapacity = DefaultQueueCapacity)

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

    ObserverConfig(
      enabled = sparkConf.getBoolean(EnabledKey, defaultValue = false),
      queueCapacity = queueCapacity
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
}
