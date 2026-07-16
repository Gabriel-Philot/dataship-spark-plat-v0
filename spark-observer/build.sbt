ThisBuild / organization := "io.dataship"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.17"

name := "dataship-spark-observer"
autoScalaLibrary := false
dependencyOverrides += "org.scala-lang" % "scala-library" % "2.13.17"
dependencyOverrides += "org.scala-lang" % "scala-reflect" % "2.13.17"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.13.17" % Provided,
  "org.apache.spark" %% "spark-core" % "4.1.2" % Provided,
  "org.apache.spark" %% "spark-sql" % "4.1.2" % Provided,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)
