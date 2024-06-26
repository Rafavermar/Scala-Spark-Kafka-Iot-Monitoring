package io

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.sql.Timestamp

/**
 * Manages Kafka data production using a centralized Kafka producer.
 * This object handles all low-level operations related to sending data to specified Kafka topics.
 */
object KafkaDataGenerator {

  // Creates the Kafka producer using properties defined in KafkaConfig
  private val producer: KafkaProducer[String, String] = createProducer()

  /**
   * Creates a KafkaProducer using the properties configured in KafkaConfig.
   *
   * @return KafkaProducer[String, String] Configured Kafka producer.
   */
  private def createProducer(): KafkaProducer[String, String] = {
    new KafkaProducer[String, String](KafkaDataGeneratorConfig.createProducerProperties())
  }

  /**
   * Sends data to a specified Kafka topic.
   *
   * @param topic The Kafka topic to which the data will be sent.
   * @param sensorId The ID of the sensor producing the data.
   * @param value The value measured by the sensor.
   * @param timestamp The timestamp of the data measurement.
   */
  def sendData(topic: String, sensorId: String, value: Double, timestamp: Timestamp): Unit = {
    val message = formatMessage(topic, sensorId, value, timestamp)
    val record = new ProducerRecord[String, String](topic, "key", message)
    println(s"Sending data to topic $topic")
    producer.send(record)
  }

  /**
   * Formats a message string based on the topic and sensor data.
   *
   * @param topic The Kafka topic for which the message is being formatted.
   * @param sensorId The ID of the sensor.
   * @param value The measured value.
   * @param timestamp The timestamp of the measurement.
   * @return String Formatted message string.
   */
  private def formatMessage(topic: String, sensorId: String, value: Double, timestamp: Timestamp): String = {
    topic match {
      case "temperature_humidity" => s"$sensorId,$value,$value,$timestamp"
      case KafkaDataGeneratorConfig.co2Topic => s"$sensorId,$value,$timestamp"
      case "soil_moisture" => s"$sensorId,$value,$timestamp"
      case _ => throw new IllegalArgumentException("Invalid topic")
    }
  }

  /**
   * Closes the Kafka producer, ensuring all resources are cleanly released.
   */
  def closeProducer(): Unit = producer.close()
}
