package services

import io.KafkaDataGeneratorConfig
import org.apache.spark.sql.{Dataset, SparkSession}
import java.sql.Timestamp

class SensorStreamManager(implicit spark: SparkSession) {
  import spark.implicits._

  def getKafkaStream(topic: String): Dataset[(String, Timestamp)] = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaDataGeneratorConfig.bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Timestamp)]
  }
}
