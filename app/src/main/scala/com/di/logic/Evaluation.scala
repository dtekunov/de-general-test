package com.di.logic

import com.di.io.KeyValuePair
import com.di.logic.Logic.{findNumberOccurringOddTimes, findNumberOccurringOddTimesImproved}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Evaluation(implicit spark: SparkSession) {

  import spark.implicits._

  /**
   * This implementation can be divided into two parts:
   *
   * 1) GroupByKey part, which is parallelizable and has O(N) time complexity,
   * but VERY ineffective, since it requires shuffling partitions
   *
   * 2) HashMap-based algorithm, which has O(N) time complexity and O(N) space complexity
   * (does not require shuffling partitions)
   */
  def algorithmV1(dataset: Dataset[Row]): Dataset[KeyValuePair] =
    algorithmV1Parent(dataset, findNumberOccurringOddTimes)

  /**
   * V1 algorithm, but the 2) step requires only O(1) space,
   * since it only uses XOP operation and no in-between data structure is used
   */
  def algorithmV1b(dataset: Dataset[Row]): Dataset[KeyValuePair] =
    algorithmV1Parent(dataset, findNumberOccurringOddTimesImproved)

  /**
   * Better approach using AggregateFunction with similar, O(N) complexity, but without data shuffling
   */
  def algorithmV2(dataset: Dataset[Row]): Dataset[KeyValuePair] = {
    val typedDataset = dataset.as[KeyValuePair]

    val aggregateFunction = Logic.AggregateValues.toColumn.name("find_odd")

    typedDataset
      .select(aggregateFunction)
      .take(1)
      .headOption
      .getOrElse(Map.empty[Int, Int])
      .toSeq
      .toDF("key", "value")
      .as[KeyValuePair]
  }

  private def algorithmV1Parent(dataset: Dataset[Row], algorithm: Iterable[Int] => Int) = {
    dataset
      .as[(Int, Int)]
      .rdd
      .groupByKey()
      .map { elem => elem._1 -> algorithm(elem._2) }
      .toDF("key", "value")
      .as[KeyValuePair]
  }
}
