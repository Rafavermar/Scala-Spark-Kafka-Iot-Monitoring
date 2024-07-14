package config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  def createSession(appName: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.databricks.delta.schema.autoMerge", "true")
      .config("spark.sql.adaptive.enabled","false")
      .config("spark.sql.streaming.checkpointLocation", "./tmp")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "False")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
      .config("spark.local.dir", "/tmp/spark-temp")
      .config("spark.executor.memory", "5g")
      .getOrCreate()
  }
}
