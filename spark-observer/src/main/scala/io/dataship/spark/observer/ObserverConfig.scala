package io.dataship.spark.observer

import org.apache.spark.SparkConf

final case class ObserverConfig(enabled: Boolean)

object ObserverConfig {
  private val EnabledKey = "spark.dataship.observer.enabled"

  def from(sparkConf: SparkConf): ObserverConfig =
    ObserverConfig(
      enabled = sparkConf.getBoolean(EnabledKey, defaultValue = false)
    )
}
