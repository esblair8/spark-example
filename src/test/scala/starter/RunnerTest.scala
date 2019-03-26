package starter

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RunnerTest extends FunSuite with SparkSessionTestWrapper with DataFrameComparer with BeforeAndAfter {

  val spark: SparkSession = initialiseTestSparkSession(this.getClass.getSimpleName)

  import spark.implicits._

  private val tableName = "tableName"

  before {
    Seq(
      InputRow("foo", "foo"),
      InputRow("foo", "foo"),
      InputRow("foo", "foo"),
      InputRow("bar", "bar"),
      InputRow("bar", "bar"),
      InputRow("baz", "baz")
    ).toDF().createOrReplaceTempView(tableName)
  }

  after()

  test("test runner works correctly") {
    val runner = new Runner()(spark)

    val expectedDf = Seq(
      OutputRow("foo", 3),
      OutputRow("bar", 2),
      OutputRow("baz", 1)
    ).toDF()

    val resultDf = runner.run()

    // print data frames to output in pretty format
    resultDf.show(false)
    expectedDf.show(false)

    assertSmallDataFrameEquality(
      resultDf,
      expectedDf,
      ignoreNullable = true,
      orderedComparison = false
    )

  }
}

case class InputRow(someColumnName: String, otherColumn: String)

case class OutputRow(someColumnName: String, count: Long)
