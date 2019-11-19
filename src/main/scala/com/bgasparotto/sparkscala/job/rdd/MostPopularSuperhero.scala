package com.bgasparotto.sparkscala.job.rdd

/** Find the superhero with the most co-appearances. */
object MostPopularSuperhero {

  // Function to extract the hero ID and number of connections from each line
  def countCoOccurrences(line: String): (Int, Int) = {
    val elements = line.split("\\s+") // Splits the line by any amount of spaces
    (elements(0).toInt, elements.length - 1)
  }

  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String): Option[(Int, String)] = {
    val fields = line.split('\"') // Splits the line based on the quotation mark
    if (fields.length > 1) {
      Some(fields(0).trim().toInt, fields(1))
    }
    else {
      None // flatmap will just discard None results, and extract data from Some results.
    }
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a SparkContext
    val conf = new SparkConf().setAppName("MostPopularSuperhero");
    val sc = new SparkContext(conf)

    // Build up a hero ID -> name RDD
    val names =
      sc.textFile("dataset/marvel-graph/Marvel-names.txt")

    /*
     * Flat map is used because a few lines will be invalid and return None. In that case, the amount of records
     * actually processed will differ than the amount of lines in the file.
     */
    val namesRdd = names.flatMap(parseNames)

    // Load up the superhero co-apperarance data
    val lines =
      sc.textFile("dataset/marvel-graph/Marvel-graph.txt")

    // Convert to (heroID, number of connections) RDD. All rows will be processed so map is used.
    val pairings = lines.map(countCoOccurrences)

    // Combine entries that span more than one line
    val totalFriendsByCharacter = pairings.reduceByKey((x, y) => x + y)

    // Flip it to # of connections, hero ID
    val flipped = totalFriendsByCharacter.map(x => (x._2, x._1))

    // Find the max # of connections
    val mostPopular = flipped.max()

    // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
    val mostPopularName = namesRdd.lookup(mostPopular._2).head
    val mostPopularId = mostPopular._2

    // Print out our answer!
    println(
      s"$mostPopularName (id $mostPopularId) is the most popular superhero with ${mostPopular._1} co-appearances."
    )

    // Top 10 most populars
    val tenMostPopular = flipped.sortBy(-_._1).take(10)
    println("10 most popular superheroes")
    tenMostPopular.foreach(hero => println(namesRdd.lookup(hero._2).head))

    // Top 10 less populars
    val tenLessPopular = flipped.sortBy(_._1).take(10)
    println("10 less popular superheroes")
    tenLessPopular.foreach(hero => println(namesRdd.lookup(hero._2).head))
  }

}
