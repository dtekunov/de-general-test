package com.di.logic

import com.di.io.KeyValuePair
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

object Logic {

  def findNumberOccuringOddTimes(numbers: Iterable[Int]): Int = {
    numbers.foldLeft(Map.empty[Int, Int]) { (acc, elem) =>
      val oldValue = acc.getOrElse(elem, 0)

      acc.updated(elem, oldValue + 1)
    }
      .find(pair => pair._2 % 2 != 0)
      .get //.get used here, because it is guaranteed to have at least one odd number
      ._1
  }

  // Hashmap's keys represent numbers and values - number of occurrences
  object AggregateValues extends Aggregator[KeyValuePair, Map[KeyValuePair, Int], Map[Int, Int]] {
    type AggregateAccumulator = Map[KeyValuePair, Int]

    def zero: AggregateAccumulator = Map.empty[KeyValuePair, Int]

    def reduce(accumulator: AggregateAccumulator, value: KeyValuePair): AggregateAccumulator = {
      val oldValue = accumulator.getOrElse(value, 0)

      accumulator.updated(value, oldValue + 1)
    }

    def merge(acc1: AggregateAccumulator, acc2: AggregateAccumulator): AggregateAccumulator = {
      acc1 ++ acc2.map { case (key, numberOfOccurrences) => key -> (numberOfOccurrences + acc1.getOrElse(key, 0)) }
    }

    def finish(reductionResult: AggregateAccumulator): Map[Int, Int] =
      reductionResult
        .filter(elem => elem._2 % 2 != 0)
        .map(elem => elem._1.key -> elem._1.value)

    def bufferEncoder: Encoder[AggregateAccumulator] = ExpressionEncoder()

    def outputEncoder: Encoder[Map[Int, Int]] = ExpressionEncoder()
  }

}
