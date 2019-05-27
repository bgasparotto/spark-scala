package com.bgasparotto.sparkscala

import org.apache.log4j._
import org.apache.spark._

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

    val context = new SparkContext("local[*]", "PurchaseByCustomer")
    val input = context.textFile("src/main/resources/dataset/orders/customer-orders.csv")

    val purchaseByCustomer = input
      .map(parseLine)
      .reduceByKey((orderX, orderY) => orderX + orderY)
      .collect()
      .sortBy(- _._2)

    purchaseByCustomer.foreach(println)
  }
}
