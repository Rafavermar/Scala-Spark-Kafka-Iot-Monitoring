package services
import models.SensorData
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import services.ObjectoConFuncionSerializable.createSensorData

import java.sql.Timestamp
import java.util.Date

object ObjectoConFuncionSerializable extends Serializable {
  def createSensorData(sampleZone: String): ((String, Timestamp)) => SensorData = (tuple: (String, Timestamp)) => SensorData(tuple._1, 2.0, tuple._2, Some(sampleZone))
}

class SensorDataProcessorTest extends AnyFunSuite {
  implicit val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()

  import spark.implicits._

  // Sample data for tests
  val sampleSensorData: Seq[(String, Timestamp)] = Seq(
    ("sensor1", new Timestamp(new Date().getTime)),
    ("sensor2", new Timestamp(new Date().getTime)),
    ("sensor3", new Timestamp(new Date().getTime))
  )

  val sampleTimestamp = new Timestamp(new Date().getTime)
  val sampleZone = "zone1"
  val processor = new SensorDataProcessor

  test("handleSensorData should convert Dataset[Tuple] to DataFrame") {
    val dataSet = spark.createDataset(sampleSensorData)
    val df: DataFrame = processor.handleSensorData(dataSet, ObjectoConFuncionSerializable.createSensorData(sampleZone))

    //ASSERT
    val rows = df.collect()
    assert(rows.nonEmpty)
    assert(rows.head.getAs[String]("sensorId") != "")
    assert(rows.head.getAs[Double]("value") == 2.0)
    assert(rows.head.getAs[Timestamp]("timestamp") === sampleTimestamp)
    assert(rows.head.getAs[String]("zoneId") === sampleZone)
  }

  test("aggregateSensorData should calculate average value") {
    val dataFrame = Seq(
      SensorData("sensor1", 2.0, sampleTimestamp, Some(sampleZone)),
      SensorData("sensor2", 6.0, sampleTimestamp, Some(sampleZone)),
      SensorData("sensor3", 9.0, sampleTimestamp, Some(sampleZone))
    ).toDF()

    val aggDf = processor.aggregateSensorData(dataFrame, "1 hour")

    // ASSERT
    val rows = aggDf.collect()
    assert(rows.nonEmpty)
    assert(rows.head.getAs[Double]("avg_value") == 5.666666666666667)
  }
}