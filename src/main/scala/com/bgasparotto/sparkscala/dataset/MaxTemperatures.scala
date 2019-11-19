package com.bgasparotto.sparkscala.dataset

import com.bgasparotto.sparkscala.parser.TemperatureParser.parseTemperature
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Find the maximum temperature by weather station for a year */
object MaxTemperatures {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession
      .builder()
      .appName("MaxTemperaturesWithDataSets")
      .getOrCreate()
    val sparkContext = session.sparkContext

    import session.implicits._
    val dataSet = sparkContext
      .textFile("dataset/weather/1800.csv")
      .map(parseTemperature)
      .toDS

    dataSet
      .select(col("stationId").as("Station"), col("temperature"))
      .filter(dataSet("entryType") === "TMAX")
      .groupBy("Station")
      .agg(max(col("temperature")).as("Maximum Temperature"))
      .orderBy(desc("Maximum Temperature"))
      .show()

    session.stop()
  }
}
