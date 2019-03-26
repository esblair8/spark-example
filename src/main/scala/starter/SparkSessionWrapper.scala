package starter

import org.apache.spark.sql.SparkSession

object SparkSessionWrapper {

  /*
    create a spark conf and set app name to something specific
    which in turn is used for creating a spark context and then an sqlContext (this is used to read hive managed tables)
   */
  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .enableHiveSupport
      .getOrCreate
  }
}