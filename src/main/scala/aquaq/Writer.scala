package aquaq

import java.io.{FileOutputStream, OutputStreamWriter}

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * class to write a data frame to various locations
  *
  * @param sqlContext sqlContext to write to hadoop distributed file system
  */
class Writer(sqlContext: SQLContext) {

  /**
    * Write to a csv on hdfs
    *
    * @param dataFrame - data frame to write
    */
  def writeToHdfs(dataFrame: DataFrame): Unit = dataFrame.coalesce(1)
    .write
    .format("com.databricks.spark.csv")
    .save("/data/home/example.csv")

  /**
    * WIP - NOT TESTED
    *
    * @param dataFrame - data frame to write
    */
  // TODO investigate running spark standalone on edge node and see where this writes to
  // This may write to a director in yarn container
  def writeToLocalFileSystem(dataFrame: DataFrame): Unit = {
    val fos = new OutputStreamWriter(new FileOutputStream("/path/to/example.csv"))
    val it = dataFrame.rdd.toLocalIterator
    while (it.hasNext) {
      fos.write(it.next.toString())
      fos.write(System.lineSeparator())
    }
    fos.close()
  }
}