package aquaq

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * This runner can be newed up, mocked, unit tested etc using scala test and/or junit
  * It reads from a hive table and aggregates the data
  * Note this uses spark 1.6
  * Spark 2 has a slightly different syntax fro writing to csv
  * Spark 2.0 also has data sets whereas spark 1.6 has data frames
  *
  * @param sqlContext - sqlContext to read from hadoop distributed file system
  */
class Runner(sqlContext: SQLContext) {

  private val tableName = "tableName"
  private val columnToGroupBy = "someColumnName"

  def run(): DataFrame = {
    val df = read()
    aggregate(df)
  }

  def read(): DataFrame = sqlContext.table(tableName)
  //    .filter(col("someColumnName") === "something else")

  def aggregate(dataFrame: DataFrame): DataFrame = dataFrame
    .groupBy(col(columnToGroupBy))
    .count

}
