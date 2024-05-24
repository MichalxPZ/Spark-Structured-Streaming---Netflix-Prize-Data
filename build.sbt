ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "put.poznan.pl.michalxpz"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / scalacOptions += "-target:jvm-1.8"

lazy val root = (project in file("."))
  .settings(
    name := "bigdata"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.3.1"
)
