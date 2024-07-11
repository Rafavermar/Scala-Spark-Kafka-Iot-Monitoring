import config.{KafkaConfig, SparkConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import processing.{CO2Processor, SoilMoistureProcessor, TemperatureHumidityProcessor}
import projectutil.ZoneDataLoader
import schemas.SensorSchemas
import services.{DataStorageService, SensorDataProcessor, SensorStreamManager}

import java.sql.Timestamp


/**
 * The Main2 object serves as the entry point for the IoT Farm Monitoring application.
 * It sets up the Spark session, initializes Delta tables, and processes streaming data from Kafka.
 *
 * Explanation of the Main2 Object:
 *
 * - setupLogging: Configures the logging levels to reduce log noise from various libraries used by Spark.
 * - initializeDeltaTables: Creates empty Delta tables with predefined schemas to initialize the storage for different sensor data types.
 * - processAndWriteCO2Data: Reads CO2 data from Kafka, processes it, adds a zone ID column, writes the raw data to a Delta table, and writes aggregated data to the console.
 * - processAndWriteTemperatureHumidityData: Reads temperature and humidity data from Kafka, processes it, adds a zone ID column, writes the raw data to a Delta table, writes merged data to Delta and JSON formats, and writes aggregated data to the console. Also writes defective sensor data to the console.
 * - processAndWriteSoilMoistureData: Reads soil moisture data from Kafka, processes it, adds a zone ID column, and writes aggregated data to the console.
 * - readKafkaStream: Reads data from a specified Kafka topic and returns it as a Dataset.
 * - writeStreamData: Writes streaming data to a specified output path in a given format with optional schema merging and checkpointing.
 * - writeStreamToConsole: Writes streaming data to the console for debugging and monitoring purposes.
 */

object Main2 extends App {

  // Setup logging configuration
  setupLogging()

  // Create a SparkSession with the provided configurations
  implicit val spark: SparkSession = SparkConfig.createSession("IoT Farm Monitoring")

  val zoneDataDF = ZoneDataLoader.loadZoneData()  // load zones data

  // Define encoders for the custom data types
  implicit val stringTimestampEncoder: Encoder[(String, Timestamp)] = Encoders.tuple(Encoders.STRING, Encoders.TIMESTAMP)
  implicit val stringStringEncoder: Encoder[(String, String)] = Encoders.tuple(Encoders.STRING, Encoders.STRING)

  // Instantiate necessary services and processors
  val sensorStreamManager = new SensorStreamManager()
  val dataStorageService = new DataStorageService()
  val sensorDataProcessor = new SensorDataProcessor()

  // Initialize Delta tables with the required schemas
  initializeDeltaTables()

  // debugging
  zoneDataDF.printSchema()
  zoneDataDF.show()

  // Start processing and writing data for each sensor type
  processAndWriteCO2Data(zoneDataDF)
  processAndWriteTemperatureHumidityData(zoneDataDF)
  processAndWriteSoilMoistureData(zoneDataDF)

  // Wait for any termination signals to stop the streaming queries
  spark.streams.awaitAnyTermination()

  /**
   * Sets up logging configuration to reduce log noise from various libraries.
   */
  private def setupLogging(): Unit = {
    val log4jConfPath = "src/main/resources/log4j.properties"
    System.setProperty("log4j.configuration", s"file:$log4jConfPath")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR)
    Logger.getLogger("io.netty").setLevel(Level.ERROR)
  }

  /**
   * Initializes Delta tables by creating empty DataFrames with predefined schemas
   * and writing them to the specified paths in Delta format.
   *
   * @param spark Implicit SparkSession instance.
   */
  private def initializeDeltaTables()(implicit spark: SparkSession): Unit = {
    val emptyTempHumDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SensorSchemas.temperatureHumiditySchema)
    val emptyCo2DF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SensorSchemas.co2Schema)
    val emptySoilMoistureDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SensorSchemas.soilMoistureSchema)

    emptyTempHumDF.write.format("delta").mode("overwrite").save("./tmp/raw_temperature_humidity_zone")
    emptyTempHumDF.write.format("delta").mode("overwrite").save("./tmp/temperature_humidity_zone_merge")
    emptyCo2DF.write.format("delta").mode("overwrite").save("./tmp/raw_co2_zone")
    emptySoilMoistureDF.write.format("delta").mode("overwrite").save("./tmp/raw_soil_moisture_zone")
  }

  /**
   * Reads, processes, and writes CO2 sensor data.
   *
   * @param spark Implicit SparkSession instance.
   */
  private def processAndWriteCO2Data(zoneDataDF: DataFrame)(implicit spark: SparkSession): Unit = {
    val co2Stream = readKafkaStream(KafkaConfig.co2Topic)
    val co2DF = new CO2Processor().processStream(co2Stream)

    // Asegura que ambos DataFrames tienen nombres de columnas alineados para el join, sin necesidad de renombrar
    val co2DFWithZone = co2DF.join(zoneDataDF, co2DF("sensorId") === zoneDataDF("sensorId"), "left_outer")
      .select(
        co2DF("sensorId"),
        co2DF("co2Level"),
        co2DF("timestamp"),
        co2DF("zoneId"), // asumiendo que co2DF ya tiene esta columna; si no, usa coalesce como se muestra abajo
        zoneDataDF("zoneName"),
        zoneDataDF("latitude"),
        zoneDataDF("longitude")
      )

    // Verifica el esquema después del join para asegurarse que las columnas son las esperadas
    co2DFWithZone.printSchema()

    // Configura el directorio de checkpoint y escribe los datos
    val checkpointLocationCO2 = "./tmp/checkpoints/co2/"
    writeStreamData(co2DFWithZone, "./tmp/raw_co2_zone", "delta", "append", checkpointLocationCO2, mergeSchema = true)

    // Calcula promedios y escribe a la consola para monitoreo
    val avgCo2DF = sensorDataProcessor.aggregateSensorData(co2DFWithZone, "1 minute", Seq("co2Level"))
    writeStreamData(avgCo2DF, null, "console", "complete", checkpointLocationCO2)
  }


  /**
   * Reads, processes, and writes temperature and humidity sensor data.
   *
   * @param spark Implicit SparkSession instance.
   */
  private def processAndWriteTemperatureHumidityData(zoneDataDF: DataFrame)(implicit spark: SparkSession): Unit = {
    val tempHumStream = readKafkaStream(KafkaConfig.temperatureHumidityTopic)
    val tempHumProcessor = new TemperatureHumidityProcessor()
    val tempHumDF = tempHumProcessor.processStream(tempHumStream)
    tempHumDF.printSchema() // Verifica el esquema después de procesar el stream

    // Realiza el join directamente sin renombrar columnas, asegurando consistencia
    val tempHumDFWithZone = tempHumDF.join(zoneDataDF, tempHumDF("sensorId") === zoneDataDF("sensorId"), "left_outer")
      .select(
        tempHumDF("sensorId"),
        tempHumDF("temperature"),
        tempHumDF("humidity"),
        tempHumDF("timestamp"),
        coalesce(tempHumDF("zoneId"), zoneDataDF("zoneId")).as("zoneId"),
        zoneDataDF("zoneName"),
        zoneDataDF("latitude"),
        zoneDataDF("longitude")
      )

    // Verifica el esquema después del join
    tempHumDFWithZone.printSchema()

    // Configura el directorio de checkpoint
    val checkpointLocationTempHum = "./tmp/checkpoints/TempHum/"

    // Escritura de datos a la tabla Delta
    writeStreamData(tempHumDFWithZone, "./tmp/raw_temperature_humidity_zone", "delta", "append", checkpointLocationTempHum, mergeSchema = true)

    // Leer los datos de nuevo para fusión y escritura en JSON
    val mergedTempHumDF = spark.readStream.format("delta").load("./tmp/raw_temperature_humidity_zone")
    writeStreamData(mergedTempHumDF, "./tmp/temperature_humidity_zone_merge", "delta", "append", "./tmp/temperature_humidity_zone_merge_chk", mergeSchema = true)
    writeStreamData(mergedTempHumDF, "./tmp/temperature_humidity_zone_merge_json", "json", "append", "./tmp/temperature_humidity_zone_merge_json_chk", mergeSchema = true)

    // Calcula promedios y escribe a la consola
    val avgSensorDataDF = sensorDataProcessor.aggregateSensorData(tempHumDFWithZone, "1 minute", Seq("temperature", "humidity"))
    writeStreamData(avgSensorDataDF, null, "console", "complete", checkpointLocationTempHum)

    // Filtro y escritura de datos de sensores defectuosos
    val defectiveZoneDF = tempHumDFWithZone.filter(col("zoneId") === "defectiveZone")
    writeStreamToConsole(defectiveZoneDF)
  }


  /**
   * Reads, processes, and writes soil moisture sensor data.
   *
   * @param spark Implicit SparkSession instance.
   */
  private def processAndWriteSoilMoistureData(zoneDataDF: DataFrame)(implicit spark: SparkSession): Unit = {
    val soilMoistureStream = readKafkaStream(KafkaConfig.soilMoistureTopic)
    val soilMoistureProcessor = new SoilMoistureProcessor()
    val soilMoistureDF = soilMoistureProcessor.processStream(soilMoistureStream)

    // Realiza el join sin renombrar las columnas para evitar errores de esquemas no coincidentes
    val soilMoistureDFWithZone = soilMoistureDF.join(zoneDataDF, soilMoistureDF("sensorId") === zoneDataDF("sensorId"), "left_outer")
      .select(
        soilMoistureDF("sensorId"),
        soilMoistureDF("soilMoisture"),
        soilMoistureDF("timestamp"),
        coalesce(soilMoistureDF("zoneId"), zoneDataDF("zoneId")).as("zoneId"),
        zoneDataDF("zoneName"),
        zoneDataDF("latitude"),
        zoneDataDF("longitude")
      )

    // Verifica el esquema después del join
    soilMoistureDFWithZone.printSchema()

    // Configura el directorio de checkpoint
    val checkpointLocationSoilMoist = "./tmp/checkpoints/SoilMoist/"

    // Escritura de datos
    writeStreamData(soilMoistureDFWithZone, "./tmp/raw_soil_moisture_zone", "delta", "append", checkpointLocationSoilMoist, mergeSchema = true)

    // Agrega datos para promedio y escribe a la consola
    val avgSoilMoistureDF = sensorDataProcessor.aggregateSensorData(soilMoistureDFWithZone, "1 minute", Seq("soilMoisture"))
    writeStreamData(avgSoilMoistureDF, null, "console", "complete", checkpointLocationSoilMoist)
  }


  /**
   * Reads a Kafka stream for the given topic and returns a Dataset of tuples containing
   * the raw data as a string and a timestamp.
   *
   * @param topic The Kafka topic to read from.
   * @param spark Implicit SparkSession instance.
   * @return Dataset of tuples (raw data, timestamp).
   */
  private def readKafkaStream(topic: String)(implicit spark: SparkSession): Dataset[(String, Timestamp)] = {
    sensorStreamManager.getKafkaStream(topic, Map("failOnDataLoss" -> "false"))
      .selectExpr("CAST(value AS STRING)", "timestamp").as[(String, Timestamp)]
  }

  /**
   * Writes the streaming data to the specified output path and format.
   *
   * @param df The DataFrame to write.
   * @param outputPath The path where the data will be written.
   * @param format The format to use (e.g., "delta", "json").
   * @param outputMode The output mode to use (e.g., "append", "complete").
   * @param checkpointLocation The checkpoint location for fault-tolerance.
   * @param mergeSchema Boolean indicating whether to merge schemas.
   * @param spark Implicit SparkSession instance.
   */
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

  /**
   * Writes the streaming data to the console for debugging and monitoring purposes.
   *
   * @param df The DataFrame to write to the console.
   * @param spark Implicit SparkSession instance.
   */
  private def writeStreamToConsole(df: Dataset[_])(implicit spark: SparkSession): Unit = {
    df.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("truncate", "false")
      .start()
  }
}
