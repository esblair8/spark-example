package starter

import org.apache.spark.sql.SparkSession

/**
  * Example Spark Application
  *
  * This example won't actually read or write any data and is only
  * used to show scala syntax and how to initialise a spark application
  *
  */
object Example {

  private val appName = "example"

  /**
    * Main Entry point called by YARN or Spark Submit
    *
    * @param args - args from command line, yarn, oozie etc
    */
  def main(args: Array[String]) {

    implicit val spark: SparkSession = SparkSessionWrapper.createSparkSession(appName)
    val result = new Runner().run()
    new Writer().writeToHdfsAsCsv(result)
    spark.stop()
  }
}
