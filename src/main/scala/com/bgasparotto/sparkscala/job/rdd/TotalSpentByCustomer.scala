package com.bgasparotto.sparkscala.job.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/** Compute the total amount spent per customer in some fake e-commerce data. */
object TotalSpentByCustomer {

  /** Convert input data to (customerID, amountSpent) tuples */
  def extractCustomerPricePairs(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a SparkContext
    val conf = new SparkConf().setAppName("TotalSpentByCustomer")
    val sc = new SparkContext(conf)

    val input = sc.textFile("dataset/orders/customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)

    val totalByCustomer = mappedInput.reduceByKey((x, y) => x + y)

    val results = totalByCustomer.collect()

    // Print the results.
    results.foreach(println)
  }

}
