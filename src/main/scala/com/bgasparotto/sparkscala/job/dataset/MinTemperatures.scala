package com.bgasparotto.sparkscala.job.dataset

import com.bgasparotto.sparkscala.parser.TemperatureParser.parseTemperature
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Find the minimum temperature by weather station */
object MinTemperatures {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession
      .builder()
      .appName("MinTemperaturesWithDataSets")
      .getOrCreate()
    val sparkContext = session.sparkContext

    import session.implicits._
    val dataSet = sparkContext
      .textFile("dataset/weather/1800.csv")
      .map(parseTemperature)
      .toDS

    dataSet
      .select(col("stationId").as("Station"), col("temperature"))
      .filter(dataSet("entryType") === "TMIN")
      .groupBy("Station")
      .agg(min(col("temperature")).as("Minimum Temperature"))
      .orderBy(asc("Minimum Temperature"))
      .show()

    session.stop()
  }
}
