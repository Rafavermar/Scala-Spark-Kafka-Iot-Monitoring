package projectutil

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import schemas.ZoneSchemas  // Asegúrate de importar el objeto correcto

object ZoneDataLoader {
  def loadAndWriteZoneData(implicit spark: SparkSession, outputPath: String): DataFrame = {
    val filePath = "src/main/resources/zones.json"
    val zonesJson = spark.read
      .option("multiLine", true)
      .option("mode", "PERMISSIVE")
      .schema(ZoneSchemas.zonesSchema)
      .json(filePath)

    val processedZones = zonesJson
      .select(explode(col("zones")).as("zone"))
      .select(
        col("zone.id").as("zoneId"),
        col("zone.name").as("zoneName"),
        explode(col("zone.sensors")).as("sensor")
      )
      .select(
        col("zoneId"),
        col("zoneName"),
        col("sensor.id").as("sensorId"),
        col("sensor.name").as("sensorName"),
        col("sensor.type").as("sensorType"),
        col("sensor.latitude").as("latitude"),
        col("sensor.longitude").as("longitude")
      )


    processedZones.write.format("delta").mode("overwrite").save(outputPath)
    spark.read.format("delta").load(outputPath)
  }
}

