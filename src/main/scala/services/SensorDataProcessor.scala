package services

import models.SensorData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import util.SensorZoneMapping

class SensorDataProcessor(implicit val spark: SparkSession) {
  import spark.implicits._

  implicit val sensorDataEncoder: Encoder[SensorData] = Encoders.product[SensorData]

  val zoneIdUdf = udf((sensorId: String) => SensorZoneMapping.sensorIdToZoneId(sensorId))

  def addZoneIdColumn(df: DataFrame): DataFrame = {
    df.withColumn("zoneId", zoneIdUdf(col("sensorId")))
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