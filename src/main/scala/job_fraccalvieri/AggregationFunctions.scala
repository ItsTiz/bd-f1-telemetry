package job_fraccalvieri

object AggregationFunctions {

  case class StrategyAcc(
                          stintMap: Map[Int, (String, Int)], // stint -> (compound, laps)
                          totalLapTime: Double,
                          totalLapTimeSq: Double,
                          totalLaps: Int
                        )

  val seqFunc = (acc: StrategyAcc, value: (Int, Double, String)) => {

    val (stint, lapTime, compound) = value

    val newMap =
      if (stint > 0) {
        val updated = acc.stintMap.get(stint) match {
          case Some((comp, laps)) => (comp, laps + 1)
          case None               => (compound, 1)
        }
        acc.stintMap + (stint -> updated)
      } else acc.stintMap

    acc.copy(
      stintMap = newMap,
      totalLapTime = acc.totalLapTime + lapTime,
      totalLapTimeSq = acc.totalLapTimeSq + lapTime * lapTime,
      totalLaps = acc.totalLaps + 1
    )
  }

  val combFunc = (a: StrategyAcc, b: StrategyAcc) => {

    val mergedMap = (a.stintMap.keySet ++ b.stintMap.keySet).map { k =>
      val v1 = a.stintMap.getOrElse(k, ("", 0))
      val v2 = b.stintMap.getOrElse(k, ("", 0))

      val compound = if (v1._1.nonEmpty) v1._1 else v2._1
      val laps = v1._2 + v2._2

      k -> (compound, laps)
    }.toMap

    StrategyAcc(
      mergedMap,
      a.totalLapTime + b.totalLapTime,
      a.totalLapTimeSq + b.totalLapTimeSq,
      a.totalLaps + b.totalLaps
    )
  }

  val mapFunc = (acc: StrategyAcc) => {

    val numStints = acc.stintMap.size

    val avgLap = acc.totalLapTime / acc.totalLaps
    val variance = acc.totalLapTimeSq / acc.totalLaps - avgLap * avgLap

    val avgStintLength =
      acc.stintMap.values.map(_._2).sum.toDouble / numStints

    val orderedCompounds =
      acc.stintMap.toList.sortBy(_._1).map(_._2._1)

    (numStints, avgLap, variance, avgStintLength, orderedCompounds)
  }
}