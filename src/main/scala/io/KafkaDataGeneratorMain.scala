package io

import java.sql.Timestamp

/**
 * Main execution logic for generating and sending Kafka data using KafkaDataGenerator.
 * Manages the lifecycle of data generation tasks and ensures the Kafka producer is properly closed.
 */
object KafkaDataGeneratorMain extends App {

  // Define the topics to which data will be sent
  val topics = KafkaDataGeneratorConfig.topics

  // Loop to generate and send data
  try {
    for (j <- 1 to 30000) {
      for (i <- 1 to 9) {
        for (topic <- topics) {
          val sensorId = if (j % 50 == 0 && i == 3) s"sensor-defective-$i" else s"sensor$i"
          val value = Math.random() * 100
          val timestamp = new Timestamp(System.currentTimeMillis())
          KafkaDataGenerator.sendData(topic, sensorId, value, timestamp)
          if (!(j % 50 == 0 && i == 3)) {
            Thread.sleep(5) // Simulate delay more efficiently
          }
        }
      }
    }
  } catch {
    case e: Exception => println(s"Error during data generation and sending: ${e.getMessage}")
  } finally {
    KafkaDataGenerator.closeProducer()
  }
}
