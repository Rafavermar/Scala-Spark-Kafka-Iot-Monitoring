import config.{KafkaConfig, SparkConfig}
import processing.{CO2Processor, SoilMoistureProcessor, TemperatureHumidityProcessor}
import services.{DataStorageService, SensorDataProcessor, SensorStreamManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object Main extends App {

  // Configuración de Spark Session
  implicit val spark: SparkSession = SparkConfig.createSession("IoT Farm Monitoring")

  // Configuración de Kafka
  val temperatureHumidityTopic = "temperature_humidity"
  val soilMoistureTopic = "soil_moisture"
  val co2Topic = KafkaConfig.co2Topic

  // Inicialización de servicios
  val sensorStreamManager = new SensorStreamManager()
  val dataStorageService = new DataStorageService()
  val sensorDataProcessor = new SensorDataProcessor()

  // Leer datos de Kafka para temperatura y humedad
  val temperatureHumidityStream = sensorStreamManager.getKafkaStream(temperatureHumidityTopic)
  val temperatureHumidityProcessor = new TemperatureHumidityProcessor()
  val temperatureHumidityDF = temperatureHumidityProcessor.processStream(temperatureHumidityStream)

  val temperatureHumidityDFWithZone = sensorDataProcessor.addZoneIdColumn(temperatureHumidityDF)

  // Escribir datos iniciales
  dataStorageService.writeData(
    temperatureHumidityDFWithZone,
    "./tmp/raw_temperature_humidity_zone",
    "delta"
  )

  // Escribir datos en streaming
  dataStorageService.writeStreamData(
    temperatureHumidityDFWithZone,
    "./tmp/raw_temperature_humidity_zone",
    "5 seconds",
    "delta",
    "append",
    "./tmp/raw_temperature_humidity_zone_chk"
  )

  // Fusión de datos en streaming
  dataStorageService.writeStreamData(
    spark.readStream.format("delta").load("./tmp/raw_temperature_humidity_zone"),
    "./tmp/temperature_humidity_zone_merge",
    "60 seconds",
    "delta",
    "append",
    "./tmp/temperature_humidity_zone_merge_chk"
  )

  // Escritura final en JSON
  dataStorageService.writeStreamData(
    spark.readStream.format("delta").load("./tmp/temperature_humidity_zone_merge"),
    "./tmp/temperature_humidity_zone_merge_json",
    "60 seconds",
    "json",
    "append",
    "./tmp/temperature_humidity_zone_merge_json_chk"
  )

  // Procesamiento y agregación de datos en tiempo real (Ejemplo: Promedio de temperatura por minuto)
  val avgTemperatureDF = sensorDataProcessor.aggregateSensorData(temperatureHumidityDFWithZone, "1 minute")

  // Escribir resultados en la consola
  dataStorageService.writeStreamData(
    avgTemperatureDF,
    null, // No necesita path para consola
    "10 seconds",
    "console",
    "complete",
    null // No necesita checkpoint para consola
  )

  // Mostrar los dispositivos que no están mapeados a una zona
  val unknownZoneDF = temperatureHumidityDFWithZone.filter(col("zoneId") === "unknown")

  dataStorageService.writeStreamData(
    unknownZoneDF,
    null, // No necesita path para consola
    "20 seconds",
    "console",
    "append",
    null // No necesita checkpoint para consola
  )

  // Procesamiento de datos de CO2
  val co2Stream = sensorStreamManager.getKafkaStream(co2Topic)
  val co2Processor = new CO2Processor()
  val co2DF = co2Processor.processStream(co2Stream)
  val avgCo2DF = sensorDataProcessor.aggregateSensorData(co2DF, "1 minute")

  // Procesamiento de datos de humedad del suelo
  val soilMoistureStream = sensorStreamManager.getKafkaStream(soilMoistureTopic)
  val soilMoistureProcessor = new SoilMoistureProcessor()
  val soilMoistureDF = soilMoistureProcessor.processStream(soilMoistureStream)
  val avgSoilMoistureDF = sensorDataProcessor.aggregateSensorData(soilMoistureDF, "1 minute")

  // Mantener la aplicación en ejecución
  spark.streams.awaitAnyTermination()
}
