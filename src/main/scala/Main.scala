import org.apache.kafka.clients.producer._
import org.apache.spark.sql.{Dataset, Row}

import java.sql.Timestamp

object Main extends App {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.streaming.Trigger

  case class SensorData(sensorId: String, value: Double, timestamp: Timestamp)

  case class SensorType(name: String, sensorDataReader: (String) => SensorData, topic: String)

  // Clase para representar los datos de un sensor de humedad del suelo
  case class SoilMoistureData(sensorId: String, soilMoisture: Double, timestamp: Timestamp)

  case class UnifiedData(sensorId: String,
                         timestamp: Timestamp,
                         temperature: Option[Double] = None,
                         humidity: Option[Double] = None,
                         co2Level: Option[Double] = None,
                         soilMoisture: Option[Double] = None)

  // Clase para representar los datos de un sensor de temperatura y humedad
  case class TemperatureHumidityData(sensorId: String, temperature: Double, humidity: Double, timestamp: Timestamp, zoneId: Option[String] = None)

  // Clase para representar los datos de un sensor de nivel de CO2
  case class CO2Data(sensorId: String, co2Level: Double, timestamp: Timestamp, zoneId: Option[String] = None)

  val sensorIdToZoneId = udf((sensorId: String) => sensorToZoneMap.getOrElse(sensorId, "unknown"))

  import org.apache.spark.sql.{DataFrame, SparkSession}
  def writeData(path: String, format: String, df: DataFrame, partitions: Seq[String] = Seq.empty, checkPointPath: Option[String] = None) = {
    val writer = df.write.format(format)
    if (partitions.nonEmpty) writer.partitionBy(partitions: _*)
    if (checkPointPath.isDefined) writer.option("checkpointLocation", checkPointPath.get)
    writer.save(path)
  }

  def readData(path: String, format: String)(implicit spark: SparkSession) =
    spark.readStream.format(format).load(path)

  def getKafkaStream(topic: String , spark: SparkSession) = {
    import spark.implicits._
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Timestamp)]
  }

  def handleSensorData(df: Dataset[(String, Timestamp)], dataCaseClass: Function1[(String, Timestamp), SensorData])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val sensorDataDf = df.map(dataCaseClass)
    sensorDataDf.withColumn("zoneId", sensorIdToZoneId(col("sensorId")))
  }

  // Configuración de Spark Session
  val spark = SparkSession.builder
    .appName("IoT Farm Monitoring")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "./tmp/checkpoint")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    // Shuffle partitions
    .config("spark.sql.shuffle.partitions", "10")
    .getOrCreate()

  spark
    .sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // Configuración de Kafka
  val kafkaBootstrapServers = "localhost:9092"
  val temperatureHumidityTopic = "temperature_humidity"
  val co2Topic = "co2"
  val soilMoistureTopic = "soil_moisture"


  // Mapeo de sensores a zonas
  // Ejemplo: sensor1 -> zona1, sensor2 -> zona2
  private type SensorId = String
  private type ZoneId = String
  private val sensorToZoneMap: Map[SensorId, ZoneId] = Map(
    "sensor1" -> "zone1", "sensor2" -> "zone1", "sensor3" -> "zone1",
    "sensor4" -> "zone2", "sensor5" -> "zone2", "sensor6" -> "zone2",
    "sensor7" -> "zone3", "sensor8" -> "zone3", "sensor9" -> "zone3")

  // Leer datos de Kafka para temperatura y humedad


  import java.sql.Timestamp


  val temperatureHumidityDF = getKafkaStream(temperatureHumidityTopic, spark).map {

    case (value, timestamp) =>
      val parts = value.split(",")
      TemperatureHumidityData(parts(0), parts(1).toDouble, parts(2).toDouble, timestamp)
  }


  val temperatureHumidityDFWithZone = temperatureHumidityDF.withColumn("zoneId", sensorIdToZoneId(col("sensorId")))

  val schema = temperatureHumidityDFWithZone.schema
  val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

  emptyDF.write
    .format("delta")
    .save("./tmp/raw_temperature_humidity_zone")

  /*
  emptyDF.write
    .format("json")
    .save("./tmp/temperature_humidity_zone_merge_json")
*/
  emptyDF.write
    .format("delta")
    .partitionBy("zoneId", "sensorId")
    .save("./tmp/temperature_humidity_zone_merge")

  temperatureHumidityDFWithZone.writeStream
    .format("delta")
    .option("checkpointLocation", "./tmp/raw_temperature_humidity_zone_chk")
    .trigger(Trigger.ProcessingTime("5 second"))
    .start("./tmp/raw_temperature_humidity_zone")

  spark.readStream
    .format("delta")
    .load("./tmp/raw_temperature_humidity_zone")
    .coalesce(1)
    .writeStream
    .option("mergeSchema", "true")
    .outputMode("append")
    .partitionBy("zoneId", "sensorId")
    .format("delta")
    .option("checkpointLocation", "./tmp/temperature_humidity_zone_merge_chk")
    .trigger(Trigger.ProcessingTime("60 second"))
    .start("./tmp/temperature_humidity_zone_merge")

  spark.readStream
    .format("delta")
    .load("./tmp/temperature_humidity_zone_merge")
    .coalesce(1)
    .writeStream
    .outputMode("append")
    .format("json")
    //.partitionBy("zoneId", "sensorId")
    .start("./tmp/temperature_humidity_zone_merge_json")


  // Procesamiento y agregación de datos en tiempo real (Ejemplo: Promedio de temperatura por minuto)
  val avgTemperatureDF = temperatureHumidityDFWithZone
    .filter($"zoneId" =!= "unknown")
    .withWatermark("timestamp", "1 minute")
    .groupBy(
      window($"timestamp".cast("timestamp"), "1 minute"),
      $"zoneId"
    )
    .agg(avg($"temperature").as("avg_temperature"))

  // Escribir resultados en la consola (puede ser almacenado en otro sistema)
  val query = avgTemperatureDF.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .trigger(Trigger.ProcessingTime("10 second"))
    .start()

  // Mostrar los dispositivos que no están mapeados a una zona
  temperatureHumidityDFWithZone.filter($"zoneId" === "unknown")
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .trigger(Trigger.ProcessingTime("20 second"))
    .start()




  val co2DF = getKafkaStream(co2Topic, spark).map {
    case (value, timestamp) =>
      val parts = value.split(",")
      CO2Data(parts(0), parts(1).toDouble, Timestamp.valueOf(parts(2)))
  }

  val avgCo2DF = co2DF
    .withWatermark("timestamp", "1 minute")
    .groupBy(
      window($"timestamp".cast("timestamp"), "1 minute"),
      $"sensorId"
    )
    .agg(avg($"co2Level").as("avg_co2Level"))

  val soilMoistureDF = getKafkaStream(soilMoistureTopic, spark).map {
    case (value, timestamp) =>
      val parts = value.split(",")
      SoilMoistureData(parts(0), parts(1).toDouble, Timestamp.valueOf(parts(2)))
  }
  val avgSolilMoistureDF = soilMoistureDF
    .withWatermark("timestamp", "1 minute")
    .groupBy(
      window($"timestamp".cast("timestamp"), "1 minute"),
      $"sensorId"
    )
    .agg(avg($"soilMoisture").as("avg_soilMoisture"))


  // Unificar los datos de los diferentes sensores
  //val unifiedData = ???

  query.awaitTermination()

}