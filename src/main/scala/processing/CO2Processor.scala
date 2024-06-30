package processing

import models.sensors.CO2Sensor
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

import java.sql.Timestamp

class CO2Processor(implicit spark: SparkSession) {
  import spark.implicits._

  private val co2Schema = StructType(Seq(
    StructField("sensorId", StringType),
    StructField("co2Level", DoubleType),
    StructField("timestamp", TimestampType),
    StructField("zoneId", StringType)
  ))

  def processStream(rawData: Dataset[(String, Timestamp)]): DataFrame = {
    rawData.map {
      case (rawData, timestamp) =>
        val parts = rawData.split(",")
        CO2Sensor(parts(0), parts(1).toDouble, timestamp)
    }.toDF()
      .withColumn("zoneId", lit("")) // Ensure zoneId is always present
      .select(col("sensorId"), col("co2Level"), col("timestamp"), col("zoneId"))
  }
}
