package io.dataship.spark.observer

import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite

final class ObserverConfigSpec extends AnyFunSuite {
  test("is disabled by default") {
    val config = ObserverConfig.from(new SparkConf(false))

    assert(!config.enabled)
    assert(config.queueCapacity == 1024)
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

  test("accepts queue capacities at the supported boundaries") {
    val minimum = ObserverConfig.from(
      new SparkConf(false)
        .set("spark.dataship.observer.queue.capacity", "1")
    )
    val maximum = ObserverConfig.from(
      new SparkConf(false)
        .set("spark.dataship.observer.queue.capacity", "65536")
    )

    assert(minimum.queueCapacity == 1)
    assert(maximum.queueCapacity == 65536)
  }

  test("rejects queue capacities outside the supported range") {
    Seq("0", "65537", "not-an-integer").foreach { value =>
      val error = intercept[IllegalArgumentException] {
        ObserverConfig.from(
          new SparkConf(false)
            .set("spark.dataship.observer.queue.capacity", value)
        )
      }

      assert(error.getMessage.contains("spark.dataship.observer.queue.capacity"))
    }
  }
}
