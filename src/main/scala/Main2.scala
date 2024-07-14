import config.{KafkaConfig, SparkConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import processing.{CO2Processor, SoilMoistureProcessor, TemperatureHumidityProcessor}
import projectutil.{CustomStreamingQueryListener, DeltaTablePaths, ZoneDataLoader}
import schemas.{SensorSchemas, ZoneSchemaFlatten}
import services.{DataStorageService, SensorDataProcessor, SensorStreamManager}

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}
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

  spark.streams.addListener(new CustomStreamingQueryListener())

  val zoneDataDF = ZoneDataLoader.loadAndWriteZoneData(spark, DeltaTablePaths.zonePath)
  // debugging
 //zoneDataDF.printSchema()
 //zoneDataDF.show(20, truncate = false)

  // Define encoders for the custom data types
  implicit val stringTimestampEncoder: Encoder[(String, Timestamp)] = Encoders.tuple(Encoders.STRING, Encoders.TIMESTAMP)
  implicit val stringStringEncoder: Encoder[(String, String)] = Encoders.tuple(Encoders.STRING, Encoders.STRING)

  // Instantiate necessary services and processors
  val sensorStreamManager = new SensorStreamManager()
  val dataStorageService = new DataStorageService()
  val sensorDataProcessor = new SensorDataProcessor()

  // Initialize Delta tables with the required schemas
  initializeDeltaTables()

  // Start processing and writing data for each sensor type
  try {
    // Iniciar procesamiento de datos
    processAndWriteSoilMoistureData(zoneDataDF)
    processAndWriteCO2Data(zoneDataDF)
    processAndWriteTemperatureHumidityData(zoneDataDF)


    spark.streams.awaitAnyTermination()
  } catch {
    case e: Exception => println(s"Error during streaming processing: ${e.getMessage}")
  } finally {
    // Punto 3 y 5: Detener consultas y limpiar recursos
    // stopActiveQueries()
    cleanUpResources()
    //spark.stop()
  }

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
    List(
      (SensorSchemas.temperatureHumiditySchema, DeltaTablePaths.temperatureHumidityPath),
      (SensorSchemas.temperatureHumiditySchema, DeltaTablePaths.temperatureHumidityMergePath),
      (SensorSchemas.co2Schema, DeltaTablePaths.co2Path),
      (SensorSchemas.soilMoistureSchema, DeltaTablePaths.soilMoisturePath),
      (ZoneSchemaFlatten.processedZoneSchema, DeltaTablePaths.zonePath)
    ).foreach { case (schema, path) =>
      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      emptyDF.write.format("delta").mode("overwrite").save(path)
    }
  }


  /**
   * Reads, processes, and writes CO2 sensor data.
   *
   * @param spark Implicit SparkSession instance.
   */
  private def processAndWriteCO2Data(zoneDataDF: DataFrame)(implicit spark: SparkSession): Unit = {
    val co2Stream = readKafkaStream(KafkaConfig.co2Topic)
    val co2DF = new CO2Processor().processStream(co2Stream)
    val zoneDataDF = ZoneDataLoader.loadAndWriteZoneData(spark, DeltaTablePaths.zonePath)
    zoneDataDF.show()

    // Join incluyendo todas las columnas necesarias de zoneDataDF
    val co2DFWithZone = co2DF.join(zoneDataDF, co2DF("sensorId") === zoneDataDF("sensorId"), "left_outer")
      .select(
        co2DF("sensorId"),
        co2DF("co2Level"),
        co2DF("timestamp"),
        zoneDataDF("zoneId"),
        zoneDataDF("sensorType"),
        zoneDataDF("zoneName"),
        zoneDataDF("latitude"),
        zoneDataDF("longitude")
      )


    // Configura el directorio de checkpoint y opciones para manejar la evolución del esquema
    val checkpointLocationCO2 = "./tmp/checkpoints/co2/"
    try {
      writeStreamData(co2DFWithZone, "./tmp/raw_co2_zone", "delta", "append", checkpointLocationCO2, "CO2_zone", mergeSchema = true, overwriteSchema = true)

      // Calcula promedios y escribe a la consola para monitoreo
      val avgCo2DF = sensorDataProcessor.aggregateSensorData(co2DFWithZone, "1 minute", Seq("co2Level"))
      writeStreamData(avgCo2DF, "./tmp/avgCO2", "console", "complete", checkpointLocationCO2, "avgCo2DF", mergeSchema = true, overwriteSchema = true)
    } catch {
      case e: Exception =>
        println(s"Failed to write due to schema evolution issues: ${e.getMessage}")
    }
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
    val zoneDataDF = ZoneDataLoader.loadAndWriteZoneData(spark, DeltaTablePaths.zonePath)

    println("Schema of tempHumDFWithZone before writing:")
    tempHumDF.printSchema()

    val tempHumDFWithZone = tempHumDF.join(zoneDataDF, tempHumDF("sensorId") === zoneDataDF("sensorId"), "left_outer")
      .select(
        tempHumDF("sensorId"),
        tempHumDF("temperature"),
        tempHumDF("humidity"),
        tempHumDF("timestamp"),
        zoneDataDF("zoneId"),
        zoneDataDF("sensorType"),
        zoneDataDF("zoneName"),
        zoneDataDF("latitude"),
        zoneDataDF("longitude")
      )
    println("Schema of tempHumDFWithZone after join:")
    tempHumDFWithZone.printSchema()

    // Configura el directorio de checkpoint y maneja la evolución del esquema
    val checkpointLocationTempHum = "./tmp/checkpoints/TempHum/"
    try {
      writeStreamData(tempHumDFWithZone, "./tmp/raw_temperature_humidity_zone", "delta", "append", checkpointLocationTempHum, "TempHum_zone",mergeSchema = true, overwriteSchema = true)

      // val mergedTempHumDF = spark.readStream.format("delta").load("./tmp/raw_temperature_humidity_zone")
      // writeStreamData(mergedTempHumDF, "./tmp/temperature_humidity_zone_merge", "delta", "append", "./tmp/temperature_humidity_zone_merge_chk", "TempHum_merge",mergeSchema = true, overwriteSchema = true)

      val avgSensorDataDF = sensorDataProcessor.aggregateSensorData(tempHumDFWithZone, "1 minute", Seq("temperature", "humidity"))
      writeStreamData(avgSensorDataDF, "./tmp/avgsensordataTH", "console", "complete", checkpointLocationTempHum,"avgSensorDataDF", mergeSchema = true, overwriteSchema = true)
    } catch {
      case e: IllegalStateException =>
        println(s"Error de estado ilegal: ${e.getMessage}. Verifica los archivos delta y los directorios de checkpoint.")
      case e: FileNotFoundException =>
        println(s"Archivo no encontrado: ${e.getMessage}. Asegúrate de que los archivos delta existan y sean accesibles.")
      case e: Exception =>
        println(s"Error inesperado: ${e.getMessage}")
    }

    // Filtro y escritura de datos de sensores defectuosos
    val defectiveZoneDF = tempHumDFWithZone.filter(col("zoneId") === "Defective Zone")
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
    val zoneDataDF = ZoneDataLoader.loadAndWriteZoneData(spark, DeltaTablePaths.zonePath)


    val soilMoistureDFWithZone = soilMoistureDF.join(zoneDataDF, soilMoistureDF("sensorId") === zoneDataDF("sensorId"), "left_outer")
      .select(
        soilMoistureDF("sensorId"),
        soilMoistureDF("soilMoisture"),
        soilMoistureDF("timestamp"),
        zoneDataDF("zoneId"),
        zoneDataDF("sensorType"),
        zoneDataDF("zoneName"),
        zoneDataDF("latitude"),
        zoneDataDF("longitude")
      )

    // Verificación del esquema post-join para asegurar la integridad de los datos
    soilMoistureDFWithZone.printSchema()

    // Establece el directorio de checkpoint y maneja la evolución del esquema
    val checkpointLocationSoilMoist = "./tmp/checkpoints/SoilMoist/"
    try {
      writeStreamData(soilMoistureDFWithZone, "./tmp/raw_soil_moisture_zone", "delta", "append", checkpointLocationSoilMoist,"SoilMoisture_zone" , mergeSchema = true, overwriteSchema = true)

      // Calcula los promedios y escribe los resultados a la consola para monitoreo en tiempo real
      val avgSoilMoistureDF = sensorDataProcessor.aggregateSensorData(soilMoistureDFWithZone, "1 minute", Seq("soilMoisture"))
      writeStreamData(avgSoilMoistureDF, "./tmp/avgsoilmoist", "console", "complete", checkpointLocationSoilMoist,"avgSoilMoisture", mergeSchema = true, overwriteSchema = true)
    } catch {
      case e: IllegalStateException =>
        println(s"Error de estado ilegal: ${e.getMessage}. Verifica los archivos delta y los directorios de checkpoint.")
      case e: FileNotFoundException =>
        println(s"Archivo no encontrado: ${e.getMessage}. Asegúrate de que los archivos delta existan y sean accesibles.")
      case e: Exception =>
        println(s"Error processing soil moisture data: ${e.getMessage}")
    }
  }

  // def stopActiveQueries(): Unit = {
  //   spark.streams.active.foreach { query =>
  //     println(s"Stopping query: ${query.name}")
  //     query.stop()
  //   }
  // }


  def cleanUpResources(): Unit = {
    val checkpointDirs = List(
      DeltaTablePaths.temperatureHumidityPath,
      DeltaTablePaths.temperatureHumidityMergePath,
      DeltaTablePaths.co2Path,
      DeltaTablePaths.soilMoisturePath,
      DeltaTablePaths.zonePath
    ).map(_ + "/checkpoints")

    checkpointDirs.foreach { dir =>
      try {
        val path = Paths.get(dir)
        if (Files.exists(path)) {
          // Convertir el stream de Java a una lista de Scala
          val pathsToDelete = Files.walk(path).toArray.toSeq.asInstanceOf[Seq[java.nio.file.Path]]
          // Ordenar en orden inverso y eliminar archivos/directorios
          pathsToDelete.sortWith(_.compareTo(_) > 0).foreach { p =>
            Files.deleteIfExists(p)
          }
          println(s"Cleaned up directory: $dir")
        }
      } catch {
        case e: Exception => println(s"Failed to clean up directory $dir: ${e.getMessage}")
      }
    }
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
  private def writeStreamData(df: Dataset[_], outputPath: String, format: String, outputMode: String, checkpointLocation: String, queryName: String, mergeSchema: Boolean = false, overwriteSchema: Boolean = false)(implicit spark: SparkSession): Unit = {
    println(s"[writeStreamData] About to write a stream to $outputPath in $format format with mode $outputMode")
    val writer = df.writeStream
      .outputMode(outputMode)
      .format(format)
      .option("checkpointLocation", checkpointLocation)
      .option("path", outputPath)
      .option("mergeSchema", mergeSchema.toString)
      .option("overwriteSchema", overwriteSchema.toString)
      .queryName(queryName)
      .trigger(Trigger.ProcessingTime("10 seconds"))

    if (outputPath != null) writer.option("path", outputPath)
    if (checkpointLocation != null) writer.option("checkpointLocation", checkpointLocation)
    if (mergeSchema) writer.option("mergeSchema", "true")
    println(s"[writeStreamData] Writing stream to $outputPath")
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
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }
}
