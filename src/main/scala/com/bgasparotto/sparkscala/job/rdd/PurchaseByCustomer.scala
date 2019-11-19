package com.bgasparotto.sparkscala.job.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Bruno Gasparotto
  */
object PurchaseByCustomer {

  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val orderValue = fields(2).toFloat
    (customerId, orderValue)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a SparkContext
    val conf = new SparkConf().setAppName("PurchaseByCustomer")
    val context = new SparkContext(conf)

    val input = context.textFile("dataset/orders/customer-orders.csv")

    val purchaseByCustomer = input
      .map(parseLine)
      .reduceByKey((orderX, orderY) => orderX + orderY)
      .collect()
      .sortBy(-_._2)

    purchaseByCustomer.foreach(println)
  }
}
