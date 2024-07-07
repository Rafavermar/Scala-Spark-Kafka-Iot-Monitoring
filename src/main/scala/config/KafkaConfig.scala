package config

import org.apache.spark.sql.SparkSession

object KafkaConfig {
  val bootstrapServers: String = "your_kafka_bootstrap_servers"
  val co2Topic: String = "co2"
  val temperatureHumidityTopic: String = "temperature_humidity"
  val soilMoistureTopic: String = "soil_moisture"

  def getKafkaStream(topic: String)(implicit spark: SparkSession) = {
    import spark.implicits._
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as rawData", "CAST(timestamp AS STRING) as timestamp")
      .as[(String, String)]
  }
}
