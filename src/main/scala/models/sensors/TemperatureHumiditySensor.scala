package models.sensors

import java.sql.Timestamp


/**
 * Represents a temperature sensor reading.
 *
 * @param sensorId Unique identifier for the sensor.
 * @param temperature Measured temperature in degrees Celsius.
 * @param humidity Measured humidity.
 * @param timestamp Time when the measurement was taken.
 */
case class TemperatureHumiditySensor(sensorId: String, temperature: Double, humidity: Double, timestamp: Timestamp, zoneId: Option[String] = None)

