package services

import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SensorStreamManager is responsible for setting up Kafka streams for sensor data.
 *
 * @param spark Implicit SparkSession instance.
 */
class SensorStreamManager()(implicit spark: SparkSession) {

  val logger = org.apache.log4j.Logger.getLogger(this.getClass)

  private val BOOTSTRAP_SERVERS_KEY = "kafka.bootstrap.servers"
  private val SUBSCRIBE_KEY = "subscribe"
  private val STARTING_OFFSETS_KEY = "startingOffsets"

  private val BOOTSTRAP_SERVERS = "localhost:9092"
  private val STARTING_OFFSESTS_VALUE = "earliest"

  private val SELECT_COLUMNS = List("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

  private def createKafkaOptionMap(topic: String, options: Map[String, String] = Map()): Map[String, String] = {
    val defaultOptions = Map(
      BOOTSTRAP_SERVERS_KEY -> BOOTSTRAP_SERVERS,
      SUBSCRIBE_KEY -> topic,
      STARTING_OFFSETS_KEY -> STARTING_OFFSESTS_VALUE
    )
    defaultOptions ++ options
  }

  def getKafkaStream(topic: String, options: Map[String, String] = Map()): Option[DataFrame] = {
    val kafkaOptions = createKafkaOptionMap(topic, options)
    val configMessage = s"Creating Kafka stream for topic: $topic using options: $kafkaOptions"
    logger.info(configMessage)

    try {
      val df = spark.readStream
        .format("kafka")
        .options(kafkaOptions)
        .load()
        .selectExpr(SELECT_COLUMNS: _*)
      Some(df)
    } catch {
      case utpe: UnknownTopicOrPartitionException =>
        logger.error(s"Error encountered during Kafka stream creation for topic: $topic", utpe)
        None
    }
  }
}