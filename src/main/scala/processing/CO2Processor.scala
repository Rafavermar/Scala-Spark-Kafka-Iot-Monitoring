package processing
import models.sensors.CO2Sensor
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import java.sql.Timestamp

class CO2Processor(implicit spark: SparkSession) {
  import spark.implicits._

  def processStream(rawData: Dataset[(String, Timestamp)]): DataFrame = {
    rawData.map {
      case (rawData, timestamp) =>
        val parts = rawData.split(",")
        CO2Sensor(parts(0), parts(1).toDouble, timestamp)
    }.toDF()
  }
}

