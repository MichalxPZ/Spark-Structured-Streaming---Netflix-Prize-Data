ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "put.poznan.pl.michalxpz"
ThisBuild / scalaVersion := "2.12.19"
ThisBuild / scalacOptions += "-target:jvm-1.8"

lazy val root = (project in file("."))
  .settings(
    name := "bigdata"
  )

val sparkVersion = "3.3.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "org.apache.kafka" % "kafka-clients" % "3.1.0",
)
