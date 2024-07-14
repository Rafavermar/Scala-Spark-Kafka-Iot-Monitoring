package io

import java.sql.Timestamp
import scala.util.Random

/**
 * The `KafkaDataGeneratorMain` object encapsulates the main execution logic for generating and sending data to Kafka topics.
 * It utilizes the `KafkaDataGenerator` to produce data for various sensors and manages the lifecycle of these data generation tasks.
 * This includes initiating the data send process to predefined Kafka topics, handling potential exceptions during the data generation
 * and sending process, and ensuring that the Kafka producer is properly closed upon completion or error.
 *
 * The data generation process simulates real-time sensor data, including scenarios where data might be duplicated or delayed,
 * to test the robustness of downstream processing systems like Spark Structured Streaming applications.
 *
 * To run this generator, simply execute this object as an application. It will start sending simulated sensor data to the configured Kafka topics.
 * Ensure that the Kafka broker is accessible and the topics are correctly configured in `KafkaDataGeneratorConfig`.
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
          val currentTime = System.currentTimeMillis()
          val baseTimestamp = new Timestamp(currentTime)

          // Send normal data
          KafkaDataGenerator.sendData(topic, sensorId, value, baseTimestamp)

          // Simulate sending duplicate data
          if (Random.nextDouble() < 0.1) { // 10% chance to send a duplicate immediately
            KafkaDataGenerator.sendData(topic, sensorId, value, new Timestamp(currentTime))
          }

          // Simulate sending delayed data within acceptable late window (5 seconds)
          if (Random.nextDouble() < 0.1) { // 10% chance to send slightly delayed data
            val slightlyDelayedTimestamp = new Timestamp(currentTime - Random.nextInt(5000)) // up to 5 seconds delay
            KafkaDataGenerator.sendData(topic, sensorId, value, slightlyDelayedTimestamp)
          }

          // Simulate sending too late data that should be ignored (10 seconds late)
          if (Random.nextDouble() < 0.05) { // 5% chance to send too late data
            val tooLateTimestamp = new Timestamp(currentTime - 10000) // 10 seconds before
            KafkaDataGenerator.sendData(topic, sensorId, value, tooLateTimestamp)
          }

          Thread.sleep(Random.nextInt(100)) // Random sleep to simulate variable inter-arrival times
        }
      }
    }
  } catch {
    case e: Exception => println(s"Error during data generation and sending: ${e.getMessage}")
  } finally {
    KafkaDataGenerator.closeProducer()
  }
}
