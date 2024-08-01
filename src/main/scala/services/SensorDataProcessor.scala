package services

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class SensorDataProcessor()(implicit spark: SparkSession) {
  import spark.implicits._

  /**
   * Aggregates sensor data over specified time windows, considering late data up to 5 minutes and removing duplicates.
   *
   * @param df DataFrame containing sensor data.
   * @param windowDuration Duration of the aggregation window, e.g., "10 minutes".
   * @param valueColumns Columns to aggregate, e.g., Seq("temperature", "humidity").
   * @return DataFrame with aggregated data.
   */
  def aggregateSensorData(df: DataFrame, windowDuration: String, valueColumns: Seq[String]): DataFrame = {
    val aggExprs = valueColumns.map(colName => avg(col(colName)).as(s"avg_$colName"))
    df
      .withWatermark("timestamp", "5 minutes")  // Handle late data up to 5 minutes
      .dropDuplicates("sensorId", "timestamp")  // Remove duplicates based on sensor ID and exact timestamp
      .groupBy(window($"timestamp", windowDuration), $"zoneId")
      .agg(aggExprs.head, aggExprs.tail: _*)
  }
}
