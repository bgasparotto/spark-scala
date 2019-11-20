package com.bgasparotto.sparkscala.job.mllib

import com.bgasparotto.sparkscala.parser.MovieParser
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.mllib.recommendation._

/**
  * This algorithm doesn't produce good results, it's just an example of the
  * library's usage and how a simpler algorithm with good data can actually
  * be more effective than fancier black box algorithms.
  */
object MovieRecommendationsALS {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val conf = new SparkConf().setAppName("MovieRecommendationsALS")
    val sc = new SparkContext(conf)

    println("Loading movie names...")
    val nameDict = MovieParser.loadMovieNames("dataset/ml-100k/u.item")

    val data = sc.textFile("dataset/ml-100k/u.data")

    val ratings = data
      .map(x => x.split('\t'))
      .map(x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble))
      .cache()

    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")

    val rank = 8
    val numIterations = 10 // Too many iterations can cause StackOverflowError

    val model = ALS.train(ratings, rank, numIterations)

    val userID = args(0).toInt

    println("\nRatings for user ID " + userID + ":")

    val userRatings = ratings.filter(x => x.user == userID)

    val myRatings = userRatings.collect()

    for (rating <- myRatings) {
      println(nameDict(rating.product.toInt) + ": " + rating.rating.toString)
    }

    println("\nTop 10 recommendations:")

    val recommendations = model.recommendProducts(userID, 10)
    for (recommendation <- recommendations) {
      println(
        nameDict(recommendation.product.toInt) + " score " + recommendation.rating
      )
    }

  }
}
