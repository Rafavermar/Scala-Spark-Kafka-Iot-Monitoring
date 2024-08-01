package projectutil

object DeltaTablePaths {
  val rootPath = "./tmp"
  val temperatureHumidityPath: String = rootPath + "/raw_temperature_humidity_zone"
  val temperatureHumidityMergePath: String = rootPath + "/temperature_humidity_zone_merge"
  val co2Path: String = rootPath + "/raw_co2_zone"
  val soilMoisturePath: String = rootPath + "/raw_soil_moisture_zone"
  val zonePath: String = rootPath + "/zones"
}