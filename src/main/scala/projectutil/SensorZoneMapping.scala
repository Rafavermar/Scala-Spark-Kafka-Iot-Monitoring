package projectutil

object SensorZoneMapping {
  type SensorId = String
  type ZoneId = String
  val sensorToZoneMap: Map[SensorId, ZoneId] = Map(
    "sensor1" -> "zone1", "sensor2" -> "zone1", "sensor3" -> "zone1",
    "sensor4" -> "zone2", "sensor5" -> "zone2", "sensor6" -> "zone2",
    "sensor7" -> "zone3", "sensor8" -> "zone3", "sensor9" -> "zone3"
  )

  def sensorIdToZoneId(sensorId: String): String = sensorToZoneMap.getOrElse(sensorId, "unknown")
}
