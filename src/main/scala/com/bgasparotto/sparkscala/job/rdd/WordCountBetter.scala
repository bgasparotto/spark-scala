package com.bgasparotto.sparkscala.job.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a SparkContext
    val conf = new SparkConf().setAppName("WordCountBetter")
    val sc = new SparkContext(conf)

    // Load each line of my book into an RDD
    val input = sc.textFile("dataset/book/book.txt")

    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))

    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.countByValue()

    // Print the results
    wordCounts.foreach(println)
  }

}
