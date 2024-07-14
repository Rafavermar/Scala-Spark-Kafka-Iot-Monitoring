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
      .config("spark.databricks.delta.schema.autoMerge", value = true)
      .config("spark.sql.adaptive.enabled","false")
      .config("spark.sql.streaming.checkpointLocation", "./tmp")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }
}
