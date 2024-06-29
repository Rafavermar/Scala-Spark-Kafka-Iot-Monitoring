package services

import java.sql.Timestamp
import models.SensorData
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.sql.types.DataTypes
import util.SensorZoneMapping
import models.sensors

class SensorDataProcessor()(implicit val spark: SparkSession) extends Serializable {
  import spark.implicits._

  // Explicit encoder for SensorData
  implicit val sensorDataEncoder: Encoder[SensorData] = Encoders.product[SensorData]

  // Register the UDF to convert sensorId to zoneId
  //val zoneIdUdf = udf((sensorId: String) => SensorZoneMapping.sensorIdToZoneId(sensorId))


  // La función handleSensorData toma un DataFrame de entrada y una función de parseo que toma una tupla (String, Timestamp) y devuelve un objeto SensorData.
  // Aquí es donde tenías el fallo, la función map espera un Encoder, no un DataFrame.
  def handleSensorData(df: Dataset[(String, Timestamp)], parseFunc: ((String, Timestamp)) => SensorData): DataFrame = {
    val sensorDataDf: Dataset[SensorData] = df.map(parseFunc)(sensorDataEncoder)
    //sensorDataDf.withColumn("zoneId", zoneIdUdf(col("sensorId")))
    // sin withColumn y sin udf
    sensorDataDf.map(sensorData => SensorData(sensorData.sensorId, sensorData.value, sensorData.timestamp, Some(SensorZoneMapping.sensorIdToZoneId(sensorData.sensorId))))
      .toDF()
  }

/*  def handleSensorData(df: Dataset[(String, Timestamp)], parseFunc: (String, Timestamp) => SensorData): DataFrame = {
    // Applying map function with an explicitly provided encoder
    val sensorDataDf: Any = df.map(parseFunc)(sensorDataEncoder)
    // Using withColumn to add a new 'zoneId' column to DataFrame using the UDF
    sensorDataDf.withColumn("zoneId", zoneIdUdf(col("sensorId")))
  }*/

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

