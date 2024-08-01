package spark

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.default.parallelism", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

}
 // With Delta Lake Support
trait SparkSessionTestWrapperWithDelta {
   lazy val spark: SparkSession = SparkSession
     .builder()
     .master("local")
     .appName("spark session")
     .config("spark.sql.shuffle.partitions", "1")
     .config("spark.default.parallelism", "1")
     .config("spark.ui.enabled", "false")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .getOrCreate()
}