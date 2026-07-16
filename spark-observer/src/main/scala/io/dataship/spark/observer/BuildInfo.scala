package io.dataship.spark.observer

object BuildInfo {
  final val PluginVersion: String = "0.1.0-SNAPSHOT"
  final val SupportedSparkVersion: String = "4.1.2"
  final val ScalaBinaryVersion: String = "2.13"
  final val ArtifactName: String =
    s"dataship-spark-observer_${ScalaBinaryVersion}-${PluginVersion}.jar"
}
