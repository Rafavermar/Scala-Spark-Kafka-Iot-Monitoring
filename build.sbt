ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "smart-farm-iot-monitoring"
  )

// Importaciones necesarias para trabajar con Spark y Kafka
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"            % "3.5.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
  // Delta Lake
  "io.delta"        %% "delta-spark"           % "3.2.0",
)
