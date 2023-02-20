package com.di.logic

import com.di.io.KeyValuePair
import com.di.logic.Logic.findNumberOccuringOddTimes
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class Evaluation(implicit spark: SparkSession) {
  import spark.implicits._

  /**
   * This implementation can be divided into two parts:
   *
   * 1) GroupByKey part, which is parallelizable and has O(N) time complexity,
   *  but VERY ineffective, since it requires shuffling partitions
   *
   *  2) HashMap-based algorithm, which has O(N) time complexity and O(N) space complexity
   *  (does not require shuffling partitions)
   */
  def algorithmV1(dataset: Dataset[Row]): Dataset[KeyValuePair]  =
    dataset
      .as[(Int, Int)]
      .rdd
      .groupByKey()
      .map { elem => elem._1 -> findNumberOccuringOddTimes(elem._2)}
      .toDF("key", "value")
      .as[KeyValuePair]

  /**
   * Better approach using AggregateFunction with similar, O(N) complexity, but without data shuffling
   */
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
