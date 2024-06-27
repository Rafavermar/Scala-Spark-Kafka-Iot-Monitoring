package io

import org.scalatest.funsuite.AnyFunSuite

class KafkaDataGeneratorConfigTest extends AnyFunSuite {
  test("bootstrapServers should read correctly from config") {
    val expected = "localhost:9092"
    assert(KafkaDataGeneratorConfig.bootstrapServers == expected)
  }

  test("co2Topic should read correctly from config") {
    val expected = "co2"
    assert(KafkaDataGeneratorConfig.co2Topic == expected)
  }

  test("temperatureHumidityTopic should read correctly from config") {
    val expected = "temperature_humidity"
    assert(KafkaDataGeneratorConfig.temperatureHumidityTopic == expected)
  }

  test("soilMoistureTopic should read correctly from config") {
    val expected = "soil_moisture"
    assert(KafkaDataGeneratorConfig.soilMoistureTopic == expected)
  }
}
