package job_fraccalvieri

object LabelingFunctions {

  def assignStrategy(
                      compounds: List[String],
                      avgStintLength: Double
                    ): String = {

    val first = compounds.headOption.getOrElse("UNKNOWN")

    if (compounds.contains("SOFT") && avgStintLength < 12)
      "AGGRESSIVE"

    else if (first == "HARD" && avgStintLength > 18)
      "CONSERVATIVE"

    else if (compounds.distinct.size == 1)
      "CONSERVATIVE"

    else
      "BALANCED"
  }
}