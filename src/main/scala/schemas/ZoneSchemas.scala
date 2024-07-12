package schemas

import org.apache.spark.sql.types._

object ZoneSchemas {
  val zonesSchema: StructType = StructType(Seq(
    StructField("zones", ArrayType(StructType(Seq(
      StructField("id", StringType),  // Updated to StringType
      StructField("name", StringType),
      StructField("sensors", ArrayType(StructType(Seq(
        StructField("id", StringType),  // Updated to StringType
        StructField("name", StringType),
        StructField("latitude", DoubleType),
        StructField("longitude", DoubleType),
        StructField("type", StringType)
      ))))
    ))))
  ))
}
