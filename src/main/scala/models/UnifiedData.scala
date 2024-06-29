package models

import java.sql.Timestamp

/**
 * Represents a unified data structure to aggregate sensor readings.
 * This model can be used to combine various sensor data types into a single record.
 *
 * @param sensorId The unique identifier of the sensor.
 * @param timestamp The timestamp common to all aggregated readings.
 * @param temperature Optional temperature data included in the aggregation.
 * @param humidity Optional humidity data included in the aggregation.
 * @param co2Level Optional CO2 level data included in the aggregation.
 * @param soilMoisture Optional soil moisture data included in the aggregation.
 */
case class UnifiedData(
                        sensorId: String,
                        timestamp: Timestamp,
                        temperature: Option[Double] = None,
                        humidity: Option[Double] = None,
                        co2Level: Option[Double] = None,
                        soilMoisture: Option[Double] = None
                      )
