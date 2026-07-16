package io.dataship.spark.observer

import java.util.Collections

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}

final class SparkDataShipDriverPlugin extends DriverPlugin {
  override def init(
      sparkContext: SparkContext,
      pluginContext: PluginContext
  ): java.util.Map[String, String] = {
    ObserverConfig.from(sparkContext.getConf)
    Collections.emptyMap[String, String]()
  }
}
