package io.dataship.spark.observer

import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite

final class ObserverConfigSpec extends AnyFunSuite {
  test("is disabled by default") {
    val config = ObserverConfig.from(new SparkConf(false))

    assert(!config.enabled)
  }

  test("is enabled by the explicit opt-in flag") {
    val sparkConf =
      new SparkConf(false).set("spark.dataship.observer.enabled", "true")

    val config = ObserverConfig.from(sparkConf)

    assert(config.enabled)
  }

  test("remains disabled when the flag is explicitly false") {
    val sparkConf =
      new SparkConf(false).set("spark.dataship.observer.enabled", "false")

    val config = ObserverConfig.from(sparkConf)

    assert(!config.enabled)
  }
}
