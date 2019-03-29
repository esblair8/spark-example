package starter

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import starter.runners.Runner
import starter.utils.TestingBase

@RunWith(classOf[JUnitRunner])
class RunnerTest extends TestingBase {

  import spark.implicits._

  private val inputTableName = "input_table"
  private val outputTableName = "output_table"

  before {
    Seq(
      InputRow("foo", "foo"),
      InputRow("foo", "foo"),
      InputRow("foo", "foo"),
      InputRow("bar", "bar"),
      InputRow("bar", "bar"),
      InputRow("baz", "baz")
    ).toDF().createOrReplaceTempView(inputTableName)

    createTable("/creat_input_table.hql")
  }

  after {

    spark.sql(s"drop table $outputTableName")
  }

  test("e2e test runner works correctly") {

    val expectedDf = Seq(
      OutputRow("foo", 3),
      OutputRow("bar", 2),
      OutputRow("baz", 1)
    ).toDF()

    //run transformation
    Runner()(spark).run()

    val resultDf = spark.table(outputTableName)

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
