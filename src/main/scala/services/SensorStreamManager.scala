package services

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SensorStreamManager is responsible for setting up Kafka streams for sensor data.
 *
 * @param spark Implicit SparkSession instance.
 */
class SensorStreamManager(implicit spark: SparkSession) {

  def getKafkaStream(topic: String, options: Map[String, String] = Map()): DataFrame = {
    val defaultOptions = Map(
      "kafka.bootstrap.servers" -> "localhost:9092",
      "subscribe" -> topic,
      "startingOffsets" -> "earliest"
    )

    val kafkaOptions = defaultOptions ++ options

    spark.readStream
      .format("kafka")
      .options(kafkaOptions)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
  }
}
