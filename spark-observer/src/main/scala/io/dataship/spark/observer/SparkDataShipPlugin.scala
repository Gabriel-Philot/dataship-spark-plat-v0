package io.dataship.spark.observer

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}

final class SparkDataShipPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin =
    new SparkDataShipDriverPlugin

  override def executorPlugin(): ExecutorPlugin =
    null
}
