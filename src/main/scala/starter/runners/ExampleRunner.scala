package starter.runners

import org.apache.spark.sql.{DataFrame, SparkSession}
import starter.io.input.SQLSourceReader
import starter.io.output.SQLSourceWriter
import starter.sources.input.InputSource
import starter.sources.output.OutputSource
import starter.traits.Runner
import starter.transformations.{CountMapper, GroupByMapper}

/**
  * Example Runner that
  *
  * @param reader      - SqlSourceReader to read in a data frame from HDFS
  * @param countMapper - Mapper that will group by and count occurrences of given value
  * @param writer      - SqlSourceWriter - will write a data frame to HDFS
  * @param spark       - SparkSession
  */
class ExampleRunner(reader: SQLSourceReader[SparkSession],
                    countMapper: GroupByMapper[DataFrame],
                    writer: SQLSourceWriter)(implicit spark: SparkSession) extends Runner {

  /**
    * Run Example Transformation and Write to HDFS
    */
  def run(): Unit = {
    val df = reader.read()
    val resultDf = countMapper.map(df)
    writer.write(resultDf)
  }
}

/**
  * Companion object that will build an instance of the Exampe Runner Object
  */
object ExampleRunner {

  /** Builds an instance on the Runner class from inout arguments
    *
    * @param spark - Spark Session
    * @return - Runner
    */
  def apply()(implicit spark: SparkSession): Runner = {
    val inputSource = new InputSource
    val reader = new SQLSourceReader[SparkSession](inputSource)
    val countMapper = new CountMapper(Seq(InputSource.someColumnName))
    val outputSource = new OutputSource
    val writer = new SQLSourceWriter(outputSource)
    new ExampleRunner(reader, countMapper, writer)
  }

}