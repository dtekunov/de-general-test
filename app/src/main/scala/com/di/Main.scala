package com.di

import com.di.input.Reader
import com.di.logic.Evaluation
import org.apache.spark.sql.SparkSession

object Main extends App {
  implicit val spark: SparkSession = SparkSession
        .builder()
        .appName("DE-General-task")
        .master("local[*]")
        .getOrCreate()

  val reader = new Reader
  val evaluation = new Evaluation

  val dataset = reader.readDataset("local")

  evaluation.algorithmV2(dataset)
}

case class Arguments(task: String = "", output: String = "")

