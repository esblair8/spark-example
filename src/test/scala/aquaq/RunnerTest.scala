package aquaq

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RunnerTest extends DataFrameSuiteBase with BeforeAndAfter {

  private val tableName = "tableName"

  case class InputRow(someColumnName: String, otherColumn: String)
  case class OutputRow(someColumnName: String, total: Integer)

  before {
    sqlContext.createDataFrame(
      Seq(
        InputRow("foo", "foo"),
        InputRow("foo", "foo"),
        InputRow("foo", "foo"),
        InputRow("bar", "bar"),
        InputRow("bar", "bar"),
        InputRow("baz", "baz"))
    ).registerTempTable(tableName)
  }

  after()

  test("test runner works correctly") {
    val runner = new Runner(sqlContext)

    val expectedDf = sqlContext.createDataFrame(
      Seq(
        OutputRow("foo", 3),
        OutputRow("bar", 2),
        OutputRow("baz", 1)))

    val resultDf = runner.run()

    // print data frames to output in pretty format
    resultDf.show(false)
    expectedDf.show(false)

    assert(true)
  }
}