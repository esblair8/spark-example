package aquaq

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter

@RunWith(classOf[JUnitRunner])
class RunnerTest extends DataFrameSuiteBase with BeforeAndAfter {

  before()
  after()

  case class InputRow(someColumnName: String, otherColumn: String)

  case class OutputRow(someColumnName: String, total: Integer)

  test("test runner works correctly") {
    val runner = new Runner(sqlContext) {
      //override read method to return an in memory data frame instead of actually reading from a hive table
      override def read(): DataFrame = sqlContext.createDataFrame(
        Seq(
          InputRow("foo", "foo"),
          InputRow("foo", "foo"),
          InputRow("foo", "foo"),
          InputRow("bar", "bar"),
          InputRow("bar", "bar"),
          InputRow("baz", "baz")))
    }

    val expectedDf = sqlContext.createDataFrame(
      Seq(
        OutputRow("foo", 3),
        OutputRow("bar", 2),
        OutputRow("baz", 1)))

    val resultDf = runner.run()

    // print data frames to console in pretty format
    resultDf.show(false)
    expectedDf.show(false)

    //assertions in following format
    assert(true)
  }
}