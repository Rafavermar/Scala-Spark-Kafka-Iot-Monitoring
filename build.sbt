ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := Versions.scala

lazy val root = (project in file("."))
  .settings(
    name := "smart-farm-iot-monitoring"
  )


libraryDependencies ++= Dependencies.spark ++ Seq(
  Dependencies.typesafeConfig,
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)

