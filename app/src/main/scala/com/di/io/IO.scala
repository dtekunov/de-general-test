package com.di.io

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

class IO(isS3: Boolean)(implicit spark: SparkSession) {

  def readDataset(inputDir: String): Dataset[Row] =
    if (!isS3) {
      val csvPart = readLocal(inputDir, "csv")
      val tsvPart = readLocal(inputDir, "tsv")
      csvPart.union(tsvPart)
    } else throw new Exception("Reading from S3 is not supported yet")


  def writeDataset(dataset: Dataset[KeyValuePair], outputDir: String): Unit =
    if (!isS3)
      dataset
        .write
        .option("header", "true")
        .option("sep", "\t")
        .mode(SaveMode.Overwrite).csv(s"$outputDir.tsv")
    else throw new Exception("Writing to S3 is not supported yet")

  private def readLocal(pathToDirectory: String, format: String) = {
    val separator = format match {
      case "csv" => ","
      case "tsv" => "\t"
      case other => throw new Exception(s"Unsupported data format $other")
    }

    val dfToRead = spark.read
      .option("sep", separator)
      .option("header", "true")
      .option("inferSchema","true")
      .csv(s"$pathToDirectory/*.$format")

    val headers = dfToRead.schema.fieldNames

    dfToRead
      .withColumnRenamed(headers(0), "key")
      .withColumnRenamed(headers(1), "value")
      .na.fill(0)
  }
}

case class KeyValuePair(key: Int, value: Int)
