package com.di

import com.di.io.IO
import com.di.logic.Evaluation
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

import scala.util.{Failure, Success}

object Main extends App {
  val logger: Logger = LogManager.getRootLogger

  val parser = new OptionParser[Arguments]("de-test") {
    opt[String]('i', "input")
      .required()
      .valueName("Path for a given input file")
      .action((value, arguments) => arguments.copy(input = value))
    opt[String]('o', "output")
      .required()
      .valueName("Path for an output file")
      .action((value, arguments) => arguments.copy(output = value))
    opt[Boolean]("is-s3")
      .valueName("Whether data should be written to s3 or locally")
      .action((value, arguments) => arguments.copy(isS3 = value))
  }

  parser.parse(args, Arguments()) match {
    case Some(arguments) => run(arguments)
    case None => logger.error("Invalid arguments provided")
  }

  private def run(args: Arguments) = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("DE-General-task")
      .master("local[*]")
      .getOrCreate()

    val io = new IO(args.isS3)
    val evaluation = new Evaluation

    io.readDataset(args.input) match {
      case Success(dataset) =>
        val result = evaluation.algorithmV2(dataset)
        io.writeDataset(result, args.output) match {
          case Success(_) =>
            logger.info("Output data successfully written")
          case Failure(ex) =>
            logger.error(s"Failed to write data due to $ex")
        }
      case Failure(ex) => throw new Exception(s"Data read failed with $ex")
    }
  }


}

case class Arguments(input: String = "",
                     output: String = "",
                     isS3: Boolean = false)

