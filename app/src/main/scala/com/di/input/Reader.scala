package com.di.input

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Reader(implicit spark: SparkSession) {
  val dataDir = "" //todo: move to config

  def readDataset(source: String): Dataset[Row] = source match {
    case "local" =>
      val csvPart = readLocal(dataDir, "csv")
      val tsvPart = readLocal(dataDir, "tsv")

      csvPart.union(tsvPart)
    case src => throw new Exception(s"Datasource $src is unknown")
  }

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
