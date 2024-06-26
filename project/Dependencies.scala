import sbt.*
// This file is used to define dependencies for the project
object Dependencies {

  val sparkSql = "org.apache.spark" %% "spark-sql" % Versions.spark % "provided"
  val sparkSqlKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.spark % "provided"
  val deltaSpark = "io.delta" %% "delta-spark" % Versions.delta

  val spark: Seq[ModuleID] = Seq(sparkSql, sparkSqlKafka, deltaSpark )


}
