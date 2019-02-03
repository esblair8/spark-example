package aquaq

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * This runner can be newed up, mocked, unit tested using scala test and/or junit etc
  * It reads from a hive table and writes to a csv file
  * Note this uses spark 1.6
  * Spark 2 has a slightly different syntax fro writing to csv
  * Spark 2.0 also has datasets whereas spark 1.6 has dataframes
  *
  * @param sqlContext - sqlContext to read from hadoop distributed file system
  */
class Runner(sqlContext: SQLContext) {

  /**
    * Runs the spark application
    */
  def run(): DataFrame = {
    val df = read()
    aggregate(df)
  }

  def read(): DataFrame = sqlContext.table("tableName")
//    .filter(col("someColumnName") === "something else")

  def aggregate(dataFrame: DataFrame): DataFrame = dataFrame
    .groupBy(col("someColumnName"))
    .count()

}
