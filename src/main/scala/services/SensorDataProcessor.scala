package services

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SensorDataProcessor is responsible for processing sensor data, including mapping sensor IDs
 * to their respective zones and aggregating sensor data over specified time windows.
 *
 * @param spark Implicit SparkSession instance.
 */
class SensorDataProcessor()(implicit spark: SparkSession) {
  import spark.implicits._

  // Mapping of sensors to zones, including defective sensors.
  private type SensorId = String
  private type ZoneId = String

  // Map of sensor IDs to their respective zone IDs.
  private val sensorToZoneMap: Map[String, String] = Map(
    "sensor1" -> "zone1", "sensor2" -> "zone1", "sensor3" -> "zone1",
    "sensor4" -> "zone2", "sensor5" -> "zone2", "sensor6" -> "zone2",
    "sensor7" -> "zone3", "sensor8" -> "zone3", "sensor9" -> "zone3",
    "sensor-defective-3" -> "defectiveZone"
  )

  /**
   * Adds a zoneId column to the given DataFrame based on the sensorId.
   *
   * @param df DataFrame containing sensor data.
   * @return DataFrame with an additional zoneId column.
   */
  def addZoneIdColumn(df: DataFrame): DataFrame = {
    val sensorToZoneMapBroadcast = spark.sparkContext.broadcast(sensorToZoneMap)
    val getZoneId = udf((sensorId: String) => sensorToZoneMapBroadcast.value.getOrElse(sensorId, "defective"))
    df.withColumn("zoneId", getZoneId(col("sensorId")))
  }

  /**
   * Aggregates sensor data over specified time windows.
   *
   * @param df DataFrame containing sensor data.
   * @param windowDuration Duration of the aggregation window.
   * @param valueColumns Columns to aggregate.
   * @return DataFrame with aggregated data.
   */
  def aggregateSensorData(df: DataFrame, windowDuration: String, valueColumns: Seq[String]): DataFrame = {
    val aggExprs = valueColumns.map(colName => avg(col(colName)).as(s"avg_$colName"))
    df
      .withWatermark("timestamp", "1 minute")
      .groupBy(window($"timestamp", windowDuration), $"zoneId")
      .agg(aggExprs.head, aggExprs.tail: _*)
  }
}