package io

import org.scalatest.funsuite.AnyFunSuite
import com.typesafe.config.{Config, ConfigFactory}


class KafkaDataGeneratorConfigTest extends AnyFunSuite {
  //private val config: Config = ConfigFactory.load()

  test("bootstrapServers should read correctly from config") {
    val expectedConfigValue = "localhost:9092"
    assert(KafkaDataGeneratorConfig.bootstrapServers == expectedConfigValue)
  }

  test("co2Topic should read correctly from config") {
    val expectedConfigValue = "co2"
        assert(KafkaDataGeneratorConfig.co2Topic == expectedConfigValue)
  }

  // TODO: Add more tests for the remaining methods in KafkaDataGeneratorConfig
}