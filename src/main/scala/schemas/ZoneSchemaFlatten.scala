package schemas

import org.apache.spark.sql.types._
object ZoneSchemaFlatten {
  val processedZoneSchema: StructType = StructType(Seq(
    StructField("zoneId", StringType),
    StructField("zoneName", StringType),
    StructField("sensorId", StringType),
    StructField("sensorName", StringType),
    StructField("sensorType", StringType),
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType)
  ))
}
