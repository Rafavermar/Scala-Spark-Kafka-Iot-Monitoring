package services

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class SensorStreamManagerTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("SensorStreamManagerTest")
    .getOrCreate()

  test("SensorStreamManager should create Kafka stream") {
    val manager = new SensorStreamManager()

    val stream = manager.getKafkaStream("test-topic")

    assert(stream.get.isStreaming)
  }
}
