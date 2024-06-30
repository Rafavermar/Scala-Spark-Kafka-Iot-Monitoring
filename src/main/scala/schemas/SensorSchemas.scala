package schemas

import org.apache.spark.sql.types._

object SensorSchemas {
  val temperatureHumiditySchema: StructType = StructType(Seq(
    StructField("sensorId", StringType),
    StructField("temperature", DoubleType),
    StructField("humidity", DoubleType),
    StructField("timestamp", TimestampType),
    StructField("zoneId", StringType)
  ))

  val co2Schema: StructType = StructType(Seq(
    StructField("sensorId", StringType),
    StructField("co2Level", DoubleType),
    StructField("timestamp", TimestampType),
    StructField("zoneId", StringType)
  ))

  val soilMoistureSchema: StructType = StructType(Seq(
    StructField("sensorId", StringType),
    StructField("soilMoisture", DoubleType),
    StructField("timestamp", TimestampType),
    StructField("zoneId", StringType)
  ))
}
