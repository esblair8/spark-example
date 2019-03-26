package starter

import java.io.{FileOutputStream, OutputStreamWriter}

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Class to write a data dataSet to various locations
  *
  * @param SparkSession spark session to write to hadoop distributed file system
  */
class Writer(implicit spark: SparkSession) {

  private val outputPath = "/data/home/example.csv"

  def writeToHdfsAsCsv(dataSet: Dataset[Row]): Unit = dataSet
    .coalesce(1)
    .write
    .format("csv")
    .save(outputPath)

  /**
    * WIP - NOT TESTED
    * TODO Investigate running spark standalone on edge node and see where this writes to as it may write to a directory in yarn container
    *
    * @param dataSet - data set to write
    */
  def writeToLocalFileSystem(dataSet: Dataset[Row]): Unit = {
    val fos = new OutputStreamWriter(new FileOutputStream(outputPath))
    val it = dataSet.rdd.toLocalIterator
    while (it.hasNext) {
      fos.write(it.next.toString())
      fos.write(System.lineSeparator())
    }
    fos.close()
  }
}