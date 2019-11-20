package com.bgasparotto.sparkscala.job.dataset

import com.bgasparotto.sparkscala.parser.MovieParser.{loadMovieNames, parseMovie}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

/** Find the movies with the most ratings. */
object PopularMoviesDataSets {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession.builder
      .appName("PopularMovies")
      .getOrCreate()

    // Read in each rating line and extract the movie ID; construct an RDD of Movie objects.
    import spark.implicits._
    val moviesDS = spark.sparkContext
      .textFile("dataset/ml-100k/u.data")
      .map(parseMovie)
      .toDS() // Convert to a DataSet

    // Some SQL-style magic to sort all movies by popularity in one line!
    val topMovieIDs = moviesDS
      .groupBy("movieID")
      .count()
      .orderBy(desc("count"))
      .cache()

    // Show the results at this point:
    /*
    |movieID|count|
    +-------+-----+
    |     50|  584|
    |    258|  509|
    |    100|  508|
     */

    topMovieIDs.show()

    // Grab the top 10
    val top10 = topMovieIDs.take(10)

    // Load up the movie ID -> name map
    val names = loadMovieNames("dataset/ml-100k/u.item")

    // Print the results
    for (result <- top10) {
      // result is just a Row at this point; we need to cast it back.
      // Each row has movieID, count as above.
      println(names(result(0).asInstanceOf[Int]) + ": " + result(1))
    }

    // Stop the session
    spark.stop()
  }
}
