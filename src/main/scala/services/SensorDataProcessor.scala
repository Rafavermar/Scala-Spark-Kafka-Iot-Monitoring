package services

import java.sql.Timestamp
import models.SensorData
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.sql.types.DataTypes
import util.SensorZoneMapping
import models.sensors

class SensorDataProcessor(implicit val spark: SparkSession) {
  import spark.implicits._

  // Explicit encoder for SensorData
  implicit val sensorDataEncoder: Encoder[SensorData] = Encoders.product[SensorData]

  // Register the UDF to convert sensorId to zoneId
  val zoneIdUdf = udf((sensorId: String) => SensorZoneMapping.sensorIdToZoneId(sensorId))

  def handleSensorData(df: Dataset[(String, Timestamp)], parseFunc: (String, Timestamp) => SensorData): DataFrame = {
    // Applying map function with an explicitly provided encoder
    val sensorDataDf = df.map(parseFunc)(sensorDataEncoder)

    // Using withColumn to add a new 'zoneId' column to DataFrame using the UDF
    sensorDataDf.withColumn("zoneId", zoneIdUdf(col("sensorId")))
  }

  def aggregateSensorData(sensorDataDF: DataFrame, timeFrame: String): DataFrame = {
    sensorDataDF
      .filter(col("zoneId") =!= "unknown")
      .withWatermark("timestamp", "1 minute")
      .groupBy(
        org.apache.spark.sql.functions.window(col("timestamp"), timeFrame),
        col("zoneId")
      )
      .agg(avg(col("value")).as("avg_value"))
  }
}

