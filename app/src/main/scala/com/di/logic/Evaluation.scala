package com.di.logic

import com.di.input.KeyValuePair
import com.di.logic.Logic.findNumberOccuringOddTimes
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class Evaluation(implicit spark: SparkSession) {
  import spark.implicits._

  def algorithmV1(dataset: Dataset[Row]): DataFrame =
    dataset
      .as[(Int, Int)]
      .rdd
      .groupByKey()
      .map { elem => elem._1 -> findNumberOccuringOddTimes(elem._2)}
      .toDF()

  def algorithmV2(dataset: Dataset[Row]): Dataset[KeyValuePair] = {
    val typedDataset = dataset.as[KeyValuePair]

    val aggregateFunction = Logic.AggregateValues.toColumn.name("find_odd")

     typedDataset
       .select(aggregateFunction)
       .take(1)
       .head //.head is safe here, because it is guaranteed to have a result
       .toSeq
       .toDF("key", "value")
       .as[KeyValuePair]
  }

}
