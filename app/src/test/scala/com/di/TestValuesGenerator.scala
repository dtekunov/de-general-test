package com.di

import com.di.io.KeyValuePair

import scala.collection.mutable.ArrayBuffer
import scala.util.Random.shuffle

case class GeneratedValues(valuesToFind: Seq[KeyValuePair], allValues: Seq[(Int, Int)])


/**
 * Test object to perform random-based unit-tests
 *
 * returns [[GeneratedValues]], which has:
 *
 * allValues - generated sample
 *
 * valuesToFind - expected result for a given sample
 */
object TestValuesGenerator {
  private val rand = new scala.util.Random
  private val nonRepeatingList = shuffle((1 to 10000).toVector).to(ArrayBuffer)

  def generateRandomTestInput: GeneratedValues = {
    val keys = 1 to 9
    keys.foldLeft(GeneratedValues(Seq.empty[KeyValuePair], Seq.empty[(Int, Int)])) { (acc, key) =>
      val numberToGenerateOddTimes = nonRepeatingList.remove(0)
      val numberToGenerateEvenTimes = nonRepeatingList.remove(0)
      val valuesToAdd = {
        val gen1 = generateValueOddTimes(key, numberToGenerateOddTimes)
        val gen2 = generateValueEvenTimes(key, numberToGenerateEvenTimes)
        gen1 ++ gen2
      }

      GeneratedValues(
        acc.valuesToFind :+ KeyValuePair(key, numberToGenerateOddTimes),
        acc.allValues ++ valuesToAdd
      )
    }

  }

  private def generateValueNTimes(key: Int, whichValueToGenerate: Int) = {
    val howManyTimes = rand.nextInt(7) //adjust for a greater sample size
    (0 to howManyTimes).map(_ => (key, whichValueToGenerate))
  }

  private def generateValueEvenTimes(key: Int, whichValueToGenerate: Int) = {
    val generatedNumbers = generateValueNTimes(key, whichValueToGenerate)
    if (generatedNumbers.size % 2 == 0) generatedNumbers
    else generatedNumbers.tail
  }

  private def generateValueOddTimes(key: Int, whichValueToGenerate: Int) = {
    val generatedNumbers = generateValueNTimes(key, whichValueToGenerate)
    if (generatedNumbers.size % 2 != 0) generatedNumbers
    else generatedNumbers.tail
  }

}
