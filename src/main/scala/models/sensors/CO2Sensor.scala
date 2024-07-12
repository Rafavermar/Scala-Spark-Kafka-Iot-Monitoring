package models.sensors

import java.sql.Timestamp

/**
 * Represents a CO2 level reading from a sensor.
 *
 * @param sensorId A unique identifier for the sensor that produced the reading.
 * @param co2Level The concentration of carbon dioxide (CO2) measured by the sensor, typically in parts per million (ppm).
 * @param timestamp The exact moment the CO2 level was recorded.
 */
case class CO2Sensor(sensorId: String, co2Level: Double, timestamp: Timestamp)
