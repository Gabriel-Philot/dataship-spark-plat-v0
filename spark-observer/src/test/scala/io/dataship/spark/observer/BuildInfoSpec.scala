package io.dataship.spark.observer

import org.scalatest.funsuite.AnyFunSuite

final class BuildInfoSpec extends AnyFunSuite {
  test("publishes the plugin version") {
    assert(BuildInfo.PluginVersion == "0.1.0-SNAPSHOT")
  }

  test("publishes the supported Spark version") {
    assert(BuildInfo.SupportedSparkVersion == "4.1.2")
  }

  test("publishes the Scala binary version") {
    assert(BuildInfo.ScalaBinaryVersion == "2.13")
  }

  test("publishes the expected artifact name") {
    assert(
      BuildInfo.ArtifactName ==
        "dataship-spark-observer_2.13-0.1.0-SNAPSHOT.jar"
    )
  }
}
