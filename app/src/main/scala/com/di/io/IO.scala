package com.di.io

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.util.{Failure, Try}

class IO(isS3: Boolean)(implicit spark: SparkSession) {

  def readDataset(inputDir: String): Try[Dataset[Row]] =
    if (!isS3) Try {
      val csvPart = readLocal(inputDir, "csv")
      val tsvPart = readLocal(inputDir, "tsv")
      csvPart.union(tsvPart)
    } else Failure(new Exception("Reading from S3 is not supported yet"))


  def writeDataset(dataset: Dataset[KeyValuePair], outputDir: String): Try[Unit] =
    if (!isS3) Try {
      dataset
        .write
        .option("header", "true")
        .option("sep", "\t")
        .mode(SaveMode.Overwrite).csv(s"$outputDir.tsv")
    } else Failure(new Exception("Writing to S3 is not supported yet"))

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
