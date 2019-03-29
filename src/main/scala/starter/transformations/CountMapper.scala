package starter.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class CountMapper(columnsToGroupBy: Seq[String]) extends GroupByMapper[DataFrame] {

  override def map(input: DataFrame): DataFrame =
    input
      .groupBy(columnsToGroupBy.map(col): _*)
      .count
}
