package models.sensors

import java.sql.Timestamp

/**
 * Represents a soil moisture reading from a sensor.
 *
 * @param sensorId A unique identifier for the sensor that produced the reading.
 * @param soilMoisture The soil moisture level measured by the sensor, typically expressed as a percentage.
 * @param timestamp The exact moment the soil moisture level was recorded.
 * @param zoneId The zone where the sensor is located.
 */
case class SoilMoistureSensor(sensorId: String, soilMoisture: Double, timestamp: Timestamp)
