package processing

import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import models.sensors.CO2Sensor
import java.sql.Timestamp

class CO2Processor(implicit spark: SparkSession) {
  def processStream(rawData: Dataset[(String, String)]): DataFrame = {
    import spark.implicits._
    rawData.map {
      case (rawData, timestamp) =>
        val parts = rawData.split(",")
        CO2Sensor(parts(0), parts(1).toDouble, Timestamp.valueOf(parts(2)))
    }.toDF()
  }
}
