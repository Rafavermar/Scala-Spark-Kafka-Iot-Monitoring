package processing

import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import models.sensors.TemperatureHumiditySensor
import java.sql.Timestamp

class TemperatureHumidityProcessor(implicit spark: SparkSession) {
  import spark.implicits._

  def processStream(rawData: Dataset[(String, Timestamp)]): DataFrame = {
    rawData.map {
      case (rawData, timestamp) =>
        val parts = rawData.split(",")
        TemperatureHumiditySensor(parts(0), parts(1).toDouble, parts(2).toDouble, timestamp)
    }.toDF()
  }
}
