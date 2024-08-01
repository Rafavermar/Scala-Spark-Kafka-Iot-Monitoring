package processing

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import spark.SparkSessionTestWrapper

import java.sql.Timestamp

class SoilMoistureProcessorTest extends AnyFunSuite with SparkSessionTestWrapper{

  test("processStream should correctly parse well-formatted data") {
    import spark.implicits._
    implicit val _spark: SparkSession = spark
    val input = Seq(
      ("sensor1,30.5,2024-06-30 12:00:00.0", Timestamp.valueOf("2024-06-30 12:00:00")),
      ("sensor2,45.0,2024-06-30 12:05:00.0", Timestamp.valueOf("2024-06-30 12:05:00"))
    ).toDS()

    val processor = new SoilMoistureProcessor()
    val result = processor.processStream(input).collect()

    val expected = Array(
      ("sensor1", 30.5, Timestamp.valueOf("2024-06-30 12:00:00.0"), ""),
      ("sensor2", 45.0, Timestamp.valueOf("2024-06-30 12:05:00.0"), "")
    )

    assert(result.map(row => (row.getString(0), row.getDouble(1), row.getTimestamp(2), row.getString(3))) === expected)
  }

  test("processStream should handle incomplete data gracefully") {
    import spark.implicits._
    implicit val _spark: SparkSession = spark
    val input = Seq(
      ("sensor1,30.5", Timestamp.valueOf("2024-06-30 12:00:00")),
      ("sensor2", Timestamp.valueOf("2024-06-30 12:05:00"))
    ).toDS()

    val processor = new SoilMoistureProcessor()
    val result = processor.processStream(input).collect()

    val expected = Array(
      ("sensor1", 30.5, Timestamp.valueOf("2024-06-30 12:00:00.0"), ""),
      ("sensor2", Double.NaN, Timestamp.valueOf("2024-06-30 12:05:00.0"), "")
    )

    assert(result.map(row => (row.getString(0), row.getDouble(1), row.getTimestamp(2), row.getString(3))) === expected)
  }

  test("processStream should handle malformed data gracefully") {
    import spark.implicits._
    implicit val _spark: SparkSession = spark
    val input = Seq(
      ("sensor1,not_a_number,2024-06-30 12:00:00.0", Timestamp.valueOf("2024-06-30 12:00:00")),
      ("sensor2,45.0,not_a_timestamp", Timestamp.valueOf("2024-06-30 12:05:00"))
    ).toDS()

    val processor = new SoilMoistureProcessor()
    val result = processor.processStream(input).collect()

    val expected = Array(
      ("sensor1", Double.NaN, Timestamp.valueOf("2024-06-30 12:00:00.0"), ""),
      ("sensor2", 45.0, Timestamp.valueOf("2024-06-30 12:05:00.0"), "")
    )

    assert(result.map(row => (row.getString(0), row.getDouble(1), row.getTimestamp(2), row.getString(3))) === expected)
  }

  test("processStream should handle empty data gracefully") {
    import spark.implicits._
    implicit val _spark: SparkSession = spark
    val input = Seq(
      ("", Timestamp.valueOf("2024-06-30 12:00:00")),
      ("", Timestamp.valueOf("2024-06-30 12:05:00"))
    ).toDS()

    val processor = new SoilMoistureProcessor()
    val result = processor.processStream(input).collect()

    val expected = Array(
      ("unknown", Double.NaN, Timestamp.valueOf("2024-06-30 12:00:00.0"), ""),
      ("unknown", Double.NaN, Timestamp.valueOf("2024-06-30 12:05:00.0"), "")
    )

    assert(result.map(row => (row.getString(0), row.getDouble(1), row.getTimestamp(2), row.getString(3))) === expected)
  }

  test("processStream should handle large volumes of data") {
    import spark.implicits._
    implicit val _spark: SparkSession = spark
    val largeInput = (1 to 10000).map(i => (s"sensor$i,${i.toDouble},2024-06-30 12:00:00.0", Timestamp.valueOf("2024-06-30 12:00:00"))).toDS()

    val processor = new SoilMoistureProcessor()
    val result = processor.processStream(largeInput).collect()

    val expected = (1 to 10000).map(i => (s"sensor$i", i.toDouble, Timestamp.valueOf("2024-06-30 12:00:00.0"), "")).toArray

    assert(result.map(row => (row.getString(0), row.getDouble(1), row.getTimestamp(2), row.getString(3))) === expected)
  }
}
