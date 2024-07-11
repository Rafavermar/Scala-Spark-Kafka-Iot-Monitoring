package projectutil

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import schemas.ZoneSchemas  // Asegúrate de importar el objeto correcto

object ZoneDataLoader {
  def loadZoneData()(implicit spark: SparkSession): DataFrame = {
    val filePath = "src/main/resources/zones.json"

    // Use the defined schema for reading the JSON
    val zonesJson = spark.read
      .option("multiLine", true)
      .option("mode", "PERMISSIVE")
      .schema(ZoneSchemas.zonesSchema)  // Apply schema
      .json(filePath)

    // Debugging output to inspect what was read
    zonesJson.printSchema()
    zonesJson.show(false)

    // Explode the nested structure within the JSON file
    zonesJson
      .select(explode(col("zones")).as("zone"))  // Explodes the 'zones' array to handle nested data
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
  }
}
