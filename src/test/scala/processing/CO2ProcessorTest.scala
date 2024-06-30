package processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class CO2ProcessorTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("CO2ProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("CO2Processor should process raw data correctly") {
    val processor = new CO2Processor()

    val rawData = Seq(
      ("sensor1,400.5", Timestamp.valueOf("2024-06-30 12:00:00")),
      ("sensor2,410.0", Timestamp.valueOf("2024-06-30 12:05:00"))
    ).toDS()

    val result = processor.processStream(rawData)

    val expected = Seq(
      ("sensor1", 400.5, Timestamp.valueOf("2024-06-30 12:00:00"), ""),
      ("sensor2", 410.0, Timestamp.valueOf("2024-06-30 12:05:00"), "")
    ).toDF("sensorId", "co2Level", "timestamp", "zoneId")

    assert(result.collect() === expected.collect())
  }
}
