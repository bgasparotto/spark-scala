package com.bgasparotto.sparkscala.job.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSortedFiltered {

  def parseCommonWordLine(line: String): String = {
    val fields = line.split("\t")
    fields(0).toLowerCase
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a SparkContext
    val conf = new SparkConf().setAppName("WordCountBetterSortedFiltered")
    val sc = new SparkContext(conf)

    // Load each line of my book into an RDD
    val input = sc.textFile("dataset/book/book.txt")

    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))

    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Filters out common words in English
    val commonWordsInput = sc.textFile("dataset/book/common-english-words.data")
    val commonWords = commonWordsInput.map(parseCommonWordLine)
    val filteredWords = lowercaseWords.subtract(commonWords)

    // Count of the occurrences of each word
    val wordCounts = filteredWords.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey()

    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }

  }

}
