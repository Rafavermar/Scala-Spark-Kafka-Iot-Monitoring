package config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  def createSession(appName: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "./tmp/checkpoint")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.sql.streaming.schemaInference", true) // Inferir esquemas de forma automática
      .config("spark.databricks.delta.schema.autoMerge", true) // Habilitar la evolución automática del esquema
      .getOrCreate()
  }
}
