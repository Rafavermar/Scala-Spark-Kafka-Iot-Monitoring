package processing

import models.sensors.SoilMoistureSensor
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Timestamp
class SoilMoistureProcessor(implicit spark: SparkSession) {
  import spark.implicits._

  def processStream(rawData: Dataset[(String, Timestamp)]): DataFrame = {
    rawData.map {
      case (rawData, timestamp) =>
        val parts = rawData.split(",")
        val sensorId = if (parts.nonEmpty) parts(0) else "unknown"
        val soilMoisture = if (parts.length > 1) parts(1).toDoubleOption.getOrElse(Double.NaN) else Double.NaN
        SoilMoistureSensor(sensorId, soilMoisture, timestamp)
    }.toDF()
  }
}
