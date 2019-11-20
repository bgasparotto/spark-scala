package com.bgasparotto.sparkscala.job.rdd

import com.bgasparotto.sparkscala.parser.MovieParser.loadMovieNames
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/** Find the movies with the most ratings. */
object PopularMoviesNicer {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a SparkContext
    val conf = new SparkConf().setAppName("PopularMoviesNicer")
    val sc = new SparkContext(conf)

    // Create a broadcast variable of our ID -> movie name map
    val nameDict = sc.broadcast(loadMovieNames("dataset/ml-100k/u.item"))

    // Read in each rating line
    val lines = sc.textFile("dataset/ml-100k/u.data")

    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))

    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey((x, y) => x + y)

    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map(x => (x._2, x._1))

    // Sort
    val sortedMovies = flipped.sortByKey()

    // Fold in the movie names from the broadcast variable
    val sortedMoviesWithNames =
      sortedMovies.map(x => (nameDict.value(x._2), x._1))

    // Collect and print results
    val results = sortedMoviesWithNames.collect()

    results.foreach(println)
  }

}
