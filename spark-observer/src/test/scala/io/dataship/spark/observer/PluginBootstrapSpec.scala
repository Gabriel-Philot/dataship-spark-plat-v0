package io.dataship.spark.observer

import org.scalatest.funsuite.AnyFunSuite

final class PluginBootstrapSpec extends AnyFunSuite {
  test("creates a fresh DataShip driver plugin") {
    val plugin = new SparkDataShipPlugin

    val firstDriver = plugin.driverPlugin()
    val secondDriver = plugin.driverPlugin()

    assert(firstDriver.isInstanceOf[SparkDataShipDriverPlugin])
    assert(secondDriver.isInstanceOf[SparkDataShipDriverPlugin])
    assert(firstDriver ne secondDriver)
  }

  test("does not create an executor plugin") {
    val plugin = new SparkDataShipPlugin

    assert(plugin.executorPlugin() == null)
  }
}
