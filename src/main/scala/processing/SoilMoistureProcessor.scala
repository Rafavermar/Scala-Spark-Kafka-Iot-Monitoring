package processing

import models.sensors.SoilMoistureSensor
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import java.sql.Timestamp

class SoilMoistureProcessor(implicit spark: SparkSession) {
  import spark.implicits._

  def processStream(rawData: Dataset[(String, String)]): DataFrame = {
    rawData.map {
      case (rawData, timestamp) =>
        val parts = rawData.split(",")
        SoilMoistureSensor(parts(0), parts(1).toDouble, Timestamp.valueOf(parts(2)))
    }.toDF()
  }
}
