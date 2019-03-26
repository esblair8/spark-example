package starter

import org.apache.spark.sql.{Dataset, SparkSession}

trait SparkSessionTestWrapper {

  def initialiseTestSparkSession(name: String): SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test session")
      .getOrCreate()
  }

}
