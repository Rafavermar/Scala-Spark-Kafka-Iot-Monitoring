package services

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class SensorDataProcessorTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("SensorDataProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("SensorDataProcessor should add zoneId column correctly") {
    val processor = new SensorDataProcessor()

    val data = Seq(
      ("sensor1", 25.0, 60.0, Timestamp.valueOf("2024-06-30 12:00:00")),
      ("sensor2", 30.0, 55.0, Timestamp.valueOf("2024-06-30 12:05:00"))
    ).toDF("sensorId", "temperature", "humidity", "timestamp")

    val result = processor.addZoneIdColumn(data)

    val expected = Seq(
      ("sensor1", 25.0, 60.0, Timestamp.valueOf("2024-06-30 12:00:00"), "zone1"),
      ("sensor2", 30.0, 55.0, Timestamp.valueOf("2024-06-30 12:05:00"), "zone1")
    ).toDF("sensorId", "temperature", "humidity", "timestamp", "zoneId")

    assert(result.collect() === expected.collect())
  }

  test("SensorDataProcessor should aggregate sensor data correctly") {
    val processor = new SensorDataProcessor()

    val data = Seq(
      ("sensor1", 25.0, 60.0, Timestamp.valueOf("2024-06-30 12:00:00"), "zone1"),
      ("sensor1", 30.0, 55.0, Timestamp.valueOf("2024-06-30 12:05:00"), "zone1")
    ).toDF("sensorId", "temperature", "humidity", "timestamp", "zoneId")

    val result = processor.aggregateSensorData(data, "1 minute", Seq("temperature", "humidity"))

    // Add more assertions based on the expected result of the aggregation
  }
}
