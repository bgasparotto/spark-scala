package com.bgasparotto.sparkscala.parser

import com.bgasparotto.sparkscala.model.Movie

object MovieParser {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(path: String): Map[Int, String] = {
    val sourceFile = FileParser.open(path)

    try {
      sourceFile
        .getLines()
        .flatMap(parseMovieName)
        .toMap
    } finally {
      sourceFile.close()
    }
  }

  def parseMovieName(line: String): Option[(Int, String)] = {
    val fields = line.split('|')
    if (fields.length > 1) {
      Some(fields(0).toInt -> fields(1))
    }
    else {
      None
    }
  }

  def parseMovie(line: String): Movie = {
    Movie(line.split("\t")(1).toInt)
  }
}
