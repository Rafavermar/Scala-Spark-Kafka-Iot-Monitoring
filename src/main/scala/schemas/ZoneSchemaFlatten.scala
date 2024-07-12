package schemas

import org.apache.spark.sql.types._
object ZoneSchemaFlatten {
  val processedZoneSchema: StructType = StructType(Seq(
    StructField("zoneId", StringType),  // Make sure this is consistent if you use zoneId as String elsewhere
    StructField("zoneName", StringType),
    StructField("sensorId", StringType),  // Updated to StringType
    StructField("sensorName", StringType),
    StructField("sensorType", StringType),
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType)
  ))
}
