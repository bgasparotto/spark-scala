package com.bgasparotto.sparkscala.parser

import com.bgasparotto.sparkscala.model.Temperature

object TemperatureParser {
  def parseTemperature(line: String): Temperature = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f
    Temperature(stationID, entryType, temperature)
  }
}
