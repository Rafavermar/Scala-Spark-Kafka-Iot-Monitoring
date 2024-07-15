import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.util.LongAccumulator
import services.SensorStreamManager

class ErrorMonitoringService(sensorStreamManager: SensorStreamManager, defectiveSensorCounter: LongAccumulator)(implicit spark: SparkSession) {
  def monitorDefectiveSensors(topic: String, outputPath: String): StreamingQuery = {
    // Utilizar SensorStreamManager para obtener el stream
    val stream = sensorStreamManager.getKafkaStream(topic)
      .selectExpr("CAST(value AS STRING) as csv")  // Suponemos que la clave no es importante para el filtrado

    // Extraer campos desde el mensaje estructurado
    val dataStream = stream.select(
      split(col("csv"), ",").getItem(0).as("sensorId"),
      split(col("csv"), ",").getItem(1).as("value"),
      split(col("csv"), ",").getItem(2).as("timestamp")
    )

    // Filtrar sensores defectuosos utilizando regex
    val defectiveStream = dataStream.filter(col("sensorId").rlike("^sensor-defective-\\d+$"))

    // Agrupar por sensorId y contar las ocurrencias
    val errorCounts = defectiveStream.groupBy("sensorId").count()

    // Mostrar los resultados en consola y/o escribirlos a un archivo
    errorCounts.writeStream
      .outputMode("complete")
      .format("console")  // Cambiar a "parquet" para escribir a archivo
      .option("path", outputPath)
      .option("checkpointLocation", outputPath + "/checkpoints")
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
      .start()
  }
}
