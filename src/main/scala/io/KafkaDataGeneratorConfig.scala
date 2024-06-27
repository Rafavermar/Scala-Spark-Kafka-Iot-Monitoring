package io

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties

object KafkaDataGeneratorConfig {
  private val config = ConfigFactory.load()
  val bootstrapServers: String = config.getString("kafka.bootstrapServers")
  val co2Topic: String = config.getString("kafka.topics.co2.name")
  // TODO: Add more configurarion configs

  val topics: Seq[String] = List("temperature_humidity", co2Topic, "soil_moisture")


  /**
   * Creates a Properties object with the necessary Kafka producer configuration.
   *
   * @return Properties Configured Kafka producer properties.
   */
  def createProducerProperties(): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }
}
