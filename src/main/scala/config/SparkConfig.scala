package config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  def createSession(appName: String, master: String, useDelta: Boolean = false, shufflePartitions: String = "10"): SparkSession = {
      val builder = SparkSession.builder()
        .appName(appName)
        .master(master)
        .config("spark.sql.shuffle.partitions", shufflePartitions)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.streaming.ui.enabled", "false")
        .config("spark.log.level", "OFF")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .config("spark.sql.adaptive.enabled","false")
        .config("spark.sql.streaming.checkpointLocation", "./tmp")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "False")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.local.dir", "/tmp/spark-temp")
        .config("spark.executor.memory", "5g")

      if (useDelta) {
        builder
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .config("spark.databricks.delta.schema.autoMerge", "true")
      }
      builder.getOrCreate()
  }
}


