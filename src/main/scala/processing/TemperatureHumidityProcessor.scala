package processing

import models.sensors.TemperatureHumiditySensor
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Timestamp

class TemperatureHumidityProcessor(implicit spark: SparkSession) {
  import spark.implicits._


  def processStream(rawData: Dataset[(String, Timestamp)]): DataFrame = {
    rawData.map {
      case (rawString, timestamp) =>
        val parts = rawString.split(",")
        TemperatureHumiditySensor(parts(0), parts(1).toDouble, parts(2).toDouble, timestamp)
    }.toDF()
      .withColumn("zoneId", lit("")) // Ensure zoneId is always present
      .select(col("sensorId"), col("temperature"), col("humidity"), col("timestamp"), col("zoneId"))
  }
}
