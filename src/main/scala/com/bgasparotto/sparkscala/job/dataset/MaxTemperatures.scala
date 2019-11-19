package com.bgasparotto.sparkscala.job.dataset

import com.bgasparotto.sparkscala.parser.TemperatureParser.parseTemperature
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MaxTemperatures {

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
      .toDS()

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
