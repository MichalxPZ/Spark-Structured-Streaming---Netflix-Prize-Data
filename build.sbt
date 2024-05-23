ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "put.poznan.pl.michalxpz"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / scalacOptions += "-target:jvm-1.8"

lazy val root = (project in file("."))
  .settings(
    name := "bigdata"
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

val sparkVersion = "3.3.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "2.3.1"
)
