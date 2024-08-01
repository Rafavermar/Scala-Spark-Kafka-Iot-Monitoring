package processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import spark.SparkSessionTestWrapper

import java.sql.Timestamp

class TemperatureHumidityProcessorTest extends AnyFunSuite with SparkSessionTestWrapper {


  test("TemperatureHumidityProcessor should process raw data correctly") {
    import spark.implicits._
    implicit val _spark: SparkSession = spark
    val processor = new TemperatureHumidityProcessor()

    val rawData = Seq(
      ("sensor1,25.0,60.0", Timestamp.valueOf("2024-06-30 12:00:00")),
      ("sensor2,30.0,55.0", Timestamp.valueOf("2024-06-30 12:05:00"))
    ).toDS()

    val result = processor.processStream(rawData)

    val expected = Seq(
      ("sensor1", 25.0, 60.0, Timestamp.valueOf("2024-06-30 12:00:00"), ""),
      ("sensor2", 30.0, 55.0, Timestamp.valueOf("2024-06-30 12:05:00"), "")
    ).toDF("sensorId", "temperature", "humidity", "timestamp", "zoneId")

    assert(result.collect() === expected.collect())
  }
}
