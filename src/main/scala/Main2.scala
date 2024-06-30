import config.{KafkaConfig, SparkConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import processing.{CO2Processor, SoilMoistureProcessor, TemperatureHumidityProcessor}
import schemas.SensorSchemas
import services.{DataStorageService, SensorDataProcessor, SensorStreamManager}

import java.sql.Timestamp

object Main2 extends App {
  setupLogging()

  implicit val spark: SparkSession = SparkConfig.createSession("IoT Farm Monitoring")

  implicit val stringTimestampEncoder: Encoder[(String, Timestamp)] = Encoders.tuple(Encoders.STRING, Encoders.TIMESTAMP)
  implicit val stringStringEncoder: Encoder[(String, String)] = Encoders.tuple(Encoders.STRING, Encoders.STRING)

  val sensorStreamManager = new SensorStreamManager()
  val dataStorageService = new DataStorageService()
  val sensorDataProcessor = new SensorDataProcessor()

  initializeDeltaTables()

  processAndWriteCO2Data()
  processAndWriteTemperatureHumidityData()
  processAndWriteSoilMoistureData()

  spark.streams.awaitAnyTermination()

  private def setupLogging(): Unit = {
    val log4jConfPath = "src/main/resources/log4j.properties"
    System.setProperty("log4j.configuration", s"file:$log4jConfPath")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR)
    Logger.getLogger("io.netty").setLevel(Level.ERROR)
  }

  private def initializeDeltaTables()(implicit spark: SparkSession): Unit = {
    val emptyTempHumDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SensorSchemas.temperatureHumiditySchema)
    val emptyCo2DF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SensorSchemas.co2Schema)
    val emptySoilMoistureDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SensorSchemas.soilMoistureSchema)

    emptyTempHumDF.write.format("delta").mode("overwrite").save("./tmp/raw_temperature_humidity_zone")
    emptyTempHumDF.write.format("delta").mode("overwrite").save("./tmp/temperature_humidity_zone_merge")
    emptyCo2DF.write.format("delta").mode("overwrite").save("./tmp/raw_co2_zone")
    emptySoilMoistureDF.write.format("delta").mode("overwrite").save("./tmp/raw_soil_moisture_zone")
  }

  private def processAndWriteCO2Data()(implicit spark: SparkSession): Unit = {
    val co2Stream = readKafkaStream(KafkaConfig.co2Topic)
    val co2Processor = new CO2Processor()
    val co2DF = co2Processor.processStream(co2Stream)
    val co2DFWithZone = sensorDataProcessor.addZoneIdColumn(co2DF)

    writeStreamData(co2DFWithZone, "./tmp/raw_co2_zone", "delta", "append", "./tmp/raw_co2_zone_chk", mergeSchema = true)

    val avgCo2DF = sensorDataProcessor.aggregateSensorData(co2DFWithZone, "1 minute", Seq("co2Level"))
    writeStreamData(avgCo2DF, null, "console", "complete", null)
  }

  private def processAndWriteTemperatureHumidityData()(implicit spark: SparkSession): Unit = {
    val tempHumStream = readKafkaStream(KafkaConfig.temperatureHumidityTopic)
    val tempHumProcessor = new TemperatureHumidityProcessor()
    val tempHumDF = tempHumProcessor.processStream(tempHumStream)
    val tempHumDFWithZone = sensorDataProcessor.addZoneIdColumn(tempHumDF)

    if (!tempHumDFWithZone.isStreaming) {
      dataStorageService.writeData(tempHumDFWithZone, "./tmp/raw_temperature_humidity_zone", "delta")
    }

    writeStreamData(tempHumDFWithZone, "./tmp/raw_temperature_humidity_zone", "delta", "append", "./tmp/raw_temperature_humidity_zone_chk", mergeSchema = true)

    val mergedTempHumDF = spark.readStream.format("delta").load("./tmp/raw_temperature_humidity_zone")
    writeStreamData(mergedTempHumDF, "./tmp/temperature_humidity_zone_merge", "delta", "append", "./tmp/temperature_humidity_zone_merge_chk", mergeSchema = true)
    writeStreamData(mergedTempHumDF, "./tmp/temperature_humidity_zone_merge_json", "json", "append", "./tmp/temperature_humidity_zone_merge_json_chk", mergeSchema = true)

    val avgSensorDataDF = sensorDataProcessor.aggregateSensorData(tempHumDFWithZone, "1 minute", Seq("temperature", "humidity"))
    writeStreamData(avgSensorDataDF, null, "console", "complete", null)

    val defectiveZoneDF = tempHumDFWithZone.filter(col("zoneId") === "defectiveZone")
    writeStreamToConsole(defectiveZoneDF)
  }

  private def processAndWriteSoilMoistureData()(implicit spark: SparkSession): Unit = {
    val soilMoistureStream = readKafkaStream(KafkaConfig.soilMoistureTopic)
    val soilMoistureProcessor = new SoilMoistureProcessor()
    val soilMoistureDF = soilMoistureProcessor.processStream(soilMoistureStream)
    val soilMoistureDFWithZone = sensorDataProcessor.addZoneIdColumn(soilMoistureDF)
    val avgSoilMoistureDF = sensorDataProcessor.aggregateSensorData(soilMoistureDFWithZone, "1 minute", Seq("soilMoisture"))

    writeStreamData(avgSoilMoistureDF, null, "console", "complete", null)
  }

  private def readKafkaStream(topic: String)(implicit spark: SparkSession): Dataset[(String, Timestamp)] = {
    sensorStreamManager.getKafkaStream(topic, Map("failOnDataLoss" -> "false"))
      .selectExpr("CAST(value AS STRING)", "timestamp").as[(String, Timestamp)]
  }

  private def writeStreamData(df: Dataset[_], outputPath: String, format: String, outputMode: String, checkpointLocation: String, mergeSchema: Boolean = false)(implicit spark: SparkSession): Unit = {
    val writer = df.writeStream
      .outputMode(outputMode)
      .format(format)
      .trigger(Trigger.ProcessingTime("30 seconds"))

    if (outputPath != null) writer.option("path", outputPath)
    if (checkpointLocation != null) writer.option("checkpointLocation", checkpointLocation)
    if (mergeSchema) writer.option("mergeSchema", "true")

    writer.start()
  }

  private def writeStreamToConsole(df: Dataset[_])(implicit spark: SparkSession): Unit = {
    df.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("truncate", "false")
      .start()
  }
}
