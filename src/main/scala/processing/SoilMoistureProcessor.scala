package processing

import models.sensors.SoilMoistureSensor
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

import java.sql.Timestamp

class SoilMoistureProcessor(implicit spark: SparkSession) {
  import spark.implicits._

  private val soilMoistureSchema = StructType(Seq(
    StructField("sensorId", StringType),
    StructField("soilMoisture", DoubleType),
    StructField("timestamp", TimestampType),
    StructField("zoneId", StringType)
  ))

  def processStream(rawData: Dataset[(String, Timestamp)]): DataFrame = {
    rawData.map {
      case (rawData, timestamp) =>
        val parts = rawData.split(",")
        SoilMoistureSensor(parts(0), parts(1).toDouble, Timestamp.valueOf(parts(2)))
    }.toDF()
      .withColumn("zoneId", lit("")) // Ensure zoneId is always present
      .select(col("sensorId"), col("soilMoisture"), col("timestamp"), col("zoneId"))
  }
}
