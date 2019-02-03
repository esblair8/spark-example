package aquaq

import java.io.{FileOutputStream, OutputStreamWriter}

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Class to write a data frame to various locations
  *
  * @param sqlContext sqlContext to write to hadoop distributed file system
  */
class Writer(sqlContext: SQLContext) {

  private val outputPath = "/data/home/example.csv"

  def writeToHdfs(dataFrame: DataFrame): Unit = dataFrame.coalesce(1)
    .write
    .format("com.databricks.spark.csv")
    .save(outputPath)

  /**
    * WIP - NOT TESTED
    * TODO Investigate running spark standalone on edge node and see where this writes to as it may write to a directory in yarn container
    *
    * @param dataFrame - data frame to write
    */
  def writeToLocalFileSystem(dataFrame: DataFrame): Unit = {
    val fos = new OutputStreamWriter(new FileOutputStream(outputPath))
    val it = dataFrame.rdd.toLocalIterator
    while (it.hasNext) {
      fos.write(it.next.toString())
      fos.write(System.lineSeparator())
    }
    fos.close()
  }
}