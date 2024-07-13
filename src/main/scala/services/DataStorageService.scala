package services

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataStorageService() {
  def writeData(df: DataFrame, path: String, format: String, checkpointPath: Option[String] = None, partitions: Seq[String] = Seq.empty): Unit = {
    val writer = df.write.format(format)
    if (partitions.nonEmpty) writer.partitionBy(partitions: _*)
    if (checkpointPath.isDefined) writer.option("checkpointLocation", checkpointPath.get)
    writer.save(path)
  }

  def writeStreamData(df: DataFrame, path: String, triggerTime: String, format: String, outputMode: String, checkpointPath: String): Unit = {
    val writer = df.writeStream
      .format(format)
      .outputMode(outputMode)
      .trigger(Trigger.ProcessingTime(triggerTime))

    if (format != "console") {
      writer.option("checkpointLocation", checkpointPath).start(path)
    } else {
      writer.start()
    }
  }
}
