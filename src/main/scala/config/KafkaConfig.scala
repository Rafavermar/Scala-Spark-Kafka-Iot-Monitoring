package config

import org.apache.spark.sql.{Dataset, SparkSession}

object KafkaConfig {
  val bootstrapServers: String = "your_kafka_bootstrap_servers"
  val co2Topic: String = "co2"
  val temperatureHumidityTopic: String = "temperature_humidity"
  val soilMoistureTopic: String = "soil_moisture"
/* Mario: Comento porque no se usa
  def getKafkaStream(topic: String)(implicit spark: SparkSession): Dataset[(String, String)] = {
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

 */
}
