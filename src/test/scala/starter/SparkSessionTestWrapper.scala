package starter

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  def initialiseTestSparkSession(name: String): SparkSession = {
    SparkSession
      .builder
      .master("local")
      .appName(name)
      .getOrCreate
  }
}