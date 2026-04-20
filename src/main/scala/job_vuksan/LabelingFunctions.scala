package job_vuksan

import job_vuksan.parser.F1DataParsing.LapTimeRecord

object LabelingFunctions {

    def assignDrivingStyle(stats: (Double, Double, Double))
        (accYMean: Double,
        brakeMean: Double,
        rpmMean: Double
    ): String = {

        val (maxAccY, avgBrake, avgRpm) = stats

        val score =
            (if (maxAccY > accYMean) 1 else 0) +
            (if (avgBrake > brakeMean) 1 else 0) +
            (if (avgRpm > rpmMean) 1 else 0)

        val style = score match {
            case s if s >= 2 => "AGRESSIVE"
            case 1           => "BALANCED"
            case _           => "CONSERVATIVE"
        }

        style
    }

    def assignTyreWearFunc(rowValue: (String, LapTimeRecord)):
        ((String, LapTimeRecord), String) = {

        val lapOnTyre = rowValue._2.tyreAgeLaps    // tyreAgeLaps
        val compound  = rowValue._2.tyreCompound      // tyreCompound
        val lapNumber = rowValue._2.lap   // absolute lap in race

        // Drop out-laps
        if (lapOnTyre == 1) {
            return (rowValue, "DROP")
        }

        // Determine optimal window
        val optimalMax = (lapNumber <= 15, compound) match {
            case (true,  "HARD")        => 16
            case (true,  "MEDIUM")      => 12
            case (true,  _)             => 6

            case (false, "SOFT")        => 5
            case (false, "MEDIUM")      => 12
            case (false, "HARD")        => 18
            case (false, "INTERMEDIATE")=> 7
            case (false, "WET")         => 5
            case _                      => 6
        }

        val tyreState = if (lapOnTyre <= optimalMax) "OPTIMAL" else "DEGRADED"

        (rowValue, tyreState)
    }

}
