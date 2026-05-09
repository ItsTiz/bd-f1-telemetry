package job_fraccalvieri.parser

object F1DataParsing {

  private val commaRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

  case class RecordKey(
                        event: String,
                        sessionType: String,
                        driverCode: String
                      )

  case class LapTimeRecord(
                            lapTime: Double,
                            stintNumber: Int,
                            lap: Int,
                            totalLaps: Int,
                            tyreCompound: String
                          )

  case class ResultRecord(
                           finalPosition: Int,
                           totalTime: Double
                         )

  def parseLaptimesRow(row: String): Option[(RecordKey, LapTimeRecord)] = {
    try {
      val cols = row.split(commaRegex).map(_.trim)

      val key = RecordKey(
        cols(0),  // event
        cols(1),  // sessionType
        cols(4)   // driverCode
      )

      val record = LapTimeRecord(
        lapTime = if (cols(7).nonEmpty && cols(7) != "None") cols(7).toDouble else 0.0,
        stintNumber = if (cols(13).nonEmpty) cols(13).toInt else 0,
        lap = cols(6).toInt,
        totalLaps = cols(2).toInt,
        tyreCompound = if (cols(11).nonEmpty && cols(11) != "None") cols(11) else ""
      )

      Some((key, record))
    } catch {
      case _: Exception => None
    }
  }

  def parseResultsRow(row: String): Option[(RecordKey, ResultRecord)] = {
    try {
      val cols = row.split(commaRegex).map(_.trim)

      val key = RecordKey(
        cols(0),
        "Race",
        cols(1)
      )

      val record = ResultRecord(
        cols(2).toInt,
        cols(3).toDouble
      )

      Some((key, record))
    } catch {
      case _: Exception => None
    }
  }
}