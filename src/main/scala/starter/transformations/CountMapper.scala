package starter.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import starter.traits.DataFrameMapper

class CountMapper(columnsToGroupBy: Seq[String]) extends DataFrameMapper[DataFrame, DataFrame] {

  override def map(input: DataFrame): DataFrame =
    input
      .groupBy(columnsToGroupBy.map(col): _*)
      .count
}