package processing

import models.sensors.SoilMoistureSensor
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Timestamp

class SoilMoistureProcessor(implicit spark: SparkSession) {
  import spark.implicits._

  def processStream(rawData: Dataset[(String, Timestamp)]): DataFrame = {
    rawData.map {
      case (rawData, timestamp) =>
        val parts = rawData.split(",")
        val sensorId = if (parts.length > 0) parts(0) else "unknown"
        val soilMoisture = try {
          if (parts.length > 1) parts(1).toDouble else Double.NaN
        } catch {
          case _: NumberFormatException => Double.NaN
        }
        val ts = try {
          if (parts.length > 2) Timestamp.valueOf(parts(2)) else timestamp
        } catch {
          case _: IllegalArgumentException => timestamp
        }

        SoilMoistureSensor(sensorId, soilMoisture, ts)
    }.toDF()
      .withColumn("zoneId", lit("")) // Ensure zoneId is always present
      .select(col("sensorId"), col("soilMoisture"), col("timestamp"), col("zoneId"))
  }
}
