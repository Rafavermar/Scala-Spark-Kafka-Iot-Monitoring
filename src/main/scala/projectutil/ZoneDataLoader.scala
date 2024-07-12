package projectutil

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import schemas.ZoneSchemas  // Asegúrate de importar el objeto correcto

object ZoneDataLoader {
  // Cambio del método para que también escriba los datos en formato Delta
  def loadAndWriteZoneData(implicit spark: SparkSession, outputPath: String): DataFrame = {
    val filePath = "src/main/resources/zones.json"

    // Use the defined schema for reading the JSON
    val zonesJson = spark.read
      .option("multiLine", true)
      .option("mode", "PERMISSIVE")
      .schema(ZoneSchemas.zonesSchema)  // Apply schema
      .json(filePath)

    // Explode the nested structure within the JSON file
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

    // Escribir el DataFrame procesado en formato Delta
    processedZones.write
      .format("delta")
      .mode("overwrite")
      .save(outputPath)

    // Retornar el DataFrame para uso inmediato si es necesario
    spark.read.format("delta").load(outputPath)

  }
}
