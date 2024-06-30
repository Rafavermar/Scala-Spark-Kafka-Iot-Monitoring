package services

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class DataStorageServiceTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("DataStorageServiceTest")
    .getOrCreate()

  import spark.implicits._

  test("DataStorageService should write and read data correctly") {
    val dataStorageService = new DataStorageService()

    val data = Seq(
      ("sensor1", 25.0, 60.0, "2024-06-30 12:00:00", "zone1")
    ).toDF("sensorId", "temperature", "humidity", "timestamp", "zoneId")

    val path = "./tmp/test_data"
    dataStorageService.writeData(data, path, "delta")

    val result = spark.read.format("delta").load(path)

    assert(result.collect() === data.collect())
  }
}
