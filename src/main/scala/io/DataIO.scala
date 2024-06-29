package io

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataIO {
  def readData(path: String, format: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format(format).load(path)
  }

  def writeData(path: String, format: String, df: DataFrame, partitions: Seq[String] = Seq.empty, checkPointPath: Option[String] = None): Unit = {
    val writer = df.write.format(format)
    if (partitions.nonEmpty) writer.partitionBy(partitions: _*)
    if (checkPointPath.isDefined) writer.option("checkpointLocation", checkPointPath.get)
    writer.save(path)
  }
}
