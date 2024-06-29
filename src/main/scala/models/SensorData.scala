package models

import java.sql.Timestamp

/**
 * Represents generic sensor data.
 * @param sensorId The unique identifier of the sensor.
 * @param value The measured value from the sensor. This could be temperature, humidity, CO2 level, etc.
 * @param timestamp The timestamp at which the measurement was taken.
 */
case class SensorData(sensorId: String, value: Double, timestamp: Timestamp, zoneId: Option[String])
