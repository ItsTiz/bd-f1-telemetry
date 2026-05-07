package job_vuksan.parser

object F1DataParsing {

    private val commaRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    private val pipeRegex = "\\|(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    private val quotes = "\""

    case class RecordKey(
        event: String,
        driveCodeName: String
    )

    case class TelemetryRecord(
        sessionType: String,
        acc_y: Double,
        brake: Int,
        rpm: Double,
        speed: Double,
        throttle: Double,
        rel_distance: String
    )

    case class LapTimeRecord(
        sessionType: String,
        lapTime: Double,
        tyreCompound: String,
        tyreAgeLaps: Int,
        lap: Int,
        totalLaps: Int,
        pitOut: String
    )

    def parseTelemetryRow(row: String): Option[(RecordKey, TelemetryRecord)] = {
        try {
            val split = row.split(commaRegex).map(_.trim)

            val event                 = split(1)
            val session_type          = split(2)
            val driver_name           = split(3)
            val acc_y                 = split(8)
            val brake                 = split(10)
            val rel_distance          = split(14)
            val rpm                   = split(15)
            val speed                 = split(16)
            val throttle              = split(17)

            val key   = RecordKey(event, driver_name)
            val value = TelemetryRecord(
                session_type,
                acc_y.toDouble,
                brake.toInt,
                rpm.toDouble,
                speed.toDouble,
                throttle.toDouble,
                rel_distance
            )
            Some((key, value))
        } catch {
            case _: Exception => None
        }
    }

    def parseLaptimesRow(row: String): Option[(RecordKey, LapTimeRecord)] = {
        try {
            val cols = row.split(commaRegex).map(_.trim)

            val key = RecordKey(cols(0), cols(4))   // keyRecord (event, driverCode)

            val record = LapTimeRecord(
                sessionType  = cols(1),
                lapTime      = if (cols(7).nonEmpty && cols(7) != "None") cols(7).toDouble else 0.0,
                tyreCompound = if (cols(11).nonEmpty && cols(11) != "None") cols(11) else "",
                tyreAgeLaps  = if (cols(12).nonEmpty) cols(12).toInt else 0,
                lap          = cols(6).toInt,
                totalLaps    = cols(2).toInt,
                pitOut       = cols(10)
            )

            Some((key, record))
        } catch {
            case _: Exception => None
        }
    }

}