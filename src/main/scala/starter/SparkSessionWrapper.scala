package starter

import org.apache.spark.sql.SparkSession

object SparkSessionWrapper {

  private val master = "local[*]"

  /*
    create a spark conf and set app name to something specific
    which in turn is used for creating a spark context and then an sqlContext (this is used to read hive managed tables)
   */
  def createSparkSession(appName: String, master: String = master): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .master(master)
      .enableHiveSupport
      .getOrCreate
  }

}
