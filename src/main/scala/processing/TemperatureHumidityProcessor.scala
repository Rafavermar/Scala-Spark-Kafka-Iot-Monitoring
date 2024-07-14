package processing

import models.sensors.TemperatureHumiditySensor
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Timestamp
class TemperatureHumidityProcessor(implicit spark: SparkSession) {
  import spark.implicits._

  def processStream(rawData: Dataset[(String, Timestamp)]): DataFrame = {
    rawData.map {
      case (rawData, timestamp) =>
        val parts = rawData.split(",")
        val sensorId = if (parts.length > 0) parts(0) else "unknown"
        val temperature = if (parts.length > 1) parts(1).toDoubleOption.getOrElse(Double.NaN) else Double.NaN
        val humidity = if (parts.length > 2) parts(2).toDoubleOption.getOrElse(Double.NaN) else Double.NaN
        TemperatureHumiditySensor(sensorId, temperature, humidity, timestamp)
    }.toDF()
  }
}