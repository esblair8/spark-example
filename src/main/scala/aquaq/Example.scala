package aquaq

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Example Spark Application
  *
  * This example won't actually read or write any data and is only
  * used to show scala syntax and how to initialise a spark application
  *
  */
object Example {

  /**
    * Main Entry point called by YARN or Spark Submit
    *
    * @param args - args from command line, yarn, oozie etc
    */
  def main(args: Array[String]) {
    /*
      create a spark conf and set app name to something specific
      which in turn is used for creating a spark context and then an sqlContext (this is used to read hive managed tables)
     */
    val sparkConf = new SparkConf().setAppName("example").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sparkContext)

    //run the spark app
    val result = new Runner(sqlContext).run()
    //write results
    new Writer(sqlContext).writeToHdfs(result)

    //stop spark context and application ends
    sparkContext.stop()
  }
}
