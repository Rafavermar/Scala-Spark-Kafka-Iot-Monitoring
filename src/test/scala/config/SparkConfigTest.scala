package config

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

// TODO: Mario: Estos tests no están bien, no se están probando las configuraciones de Spark
// Se está ptobando que Spark crea bien la sesion
class SparkConfigTest extends AnyFunSuite {

  ignore("SparkSession should be correctly configured") {
    val spark = SparkSession.builder()
      .appName("SparkConfigTest")
      .master("local[*]")
      .config("spark.executor.memory", "2g")
      .config("spark.sql.shuffle.partitions", "10")
      .getOrCreate()

    assert(spark.conf.get("spark.app.name") == "SparkConfigTest")
    assert(spark.conf.get("spark.executor.memory") == "2g")
    assert(spark.conf.get("spark.sql.shuffle.partitions") == "10")

    spark.stop()
  }

  ignore("SparkSession should handle invalid configuration gracefully") {
    val thrown = intercept[NumberFormatException] {
      val spark = SparkSession.builder()
        .appName("SparkConfigTest")
        .master("local[*]")
        .config("spark.executor.memory", "invalid_value")
        .getOrCreate()
      spark.stop()
    }
    assert(thrown.getMessage.contains("Failed to parse byte string: invalid_value"))
  }

  ignore("SparkSession should handle dynamic allocation settings") {
    val spark = SparkSession.builder()
      .appName("SparkConfigTest")
      .master("local[*]")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.dynamicAllocation.minExecutors", "2")
      .config("spark.dynamicAllocation.maxExecutors", "10")
      .getOrCreate()

    assert(spark.conf.get("spark.dynamicAllocation.enabled") == "true")
    assert(spark.conf.get("spark.dynamicAllocation.minExecutors") == "2")
    assert(spark.conf.get("spark.dynamicAllocation.maxExecutors") == "10")

    spark.stop()
  }
}
