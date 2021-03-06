package starter

import org.apache.spark.sql.SparkSession
import starter.runners.ExampleRunner

/**
  * Example Spark Application
  * To be used as a layout Guide
  */
object Example {

  def main(args: Array[String]) {

    implicit val spark: SparkSession = SparkSessionWrapper.createSparkSession(this.getClass.getSimpleName)
    ExampleRunner().run()
    spark.stop()
  }
}
