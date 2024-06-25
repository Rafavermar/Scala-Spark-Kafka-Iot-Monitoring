package io

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.sql.Timestamp
import java.util.Properties

object KafkaDataGenerator {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  def sendData(topic: String, sensorId: String, value: Double, timestamp: Timestamp): Unit = {
    // message definition based on topic
    val message = topic match {
      case "temperature_humidity" => s"$sensorId,$value,$value,$timestamp"
      case "co2" => s"$sensorId,$value,$timestamp"
      case "soil_moisture" => s"$sensorId,$value,$timestamp"
      case _ => throw new Exception("Invalid topic")
    }
    val record = new ProducerRecord[String, String](topic, "key", message)
    println(s"Sending data to topic $topic")
    producer.send(record)
  }

  def generateAndSendData(topic: String, sensorId: String): Unit = {
    val timestamp = new Timestamp(System.currentTimeMillis())
    sendData(topic, sensorId, Math.random() * 100, timestamp)
  }

  def main(args: Array[String]): Unit = {
    val topics = List("temperature_humidity", "co2", "soil_moisture")
    for (j <- 1 to 30000) {
      for (topic <- topics) {
        for (i <- 1 to 9) {
          // Cada 500 registros, se envía un mensaje con un sensor desconocido
          if (j % 50 == 0 && i == 3) {
            generateAndSendData(topic, s"sensor-chungo-$i")
          } else {
            generateAndSendData(topic, s"sensor$i")
            Thread.sleep(5) // delay for the demo purpose
          }
        }
      }
    }
    producer.close()
  }
}