package job_vuksan

object AggregationFunctions {
    // Defining the accumulator case class (maxAccY, brakeSum, rpmSum, totalHighSpeedThrottleRows, totalRows)
    case class TelemetryAcc(
       maxAccY: List[Double],
       brakeSum: Double,
       rpmSum: Double,
       totalHighSpeedThrottleRows: Long,
       totalRows: Long
    )

    case class DrivingStyleAcc(
        accSum: Double,
        brakeSum: Double,
        rpmSum: Double,
        totalRows: Long
    )

    // sequencing function on (acc_y, brake, rpm, speed, throttle) to calc average and give as a result (max_acc_y, sum_brake, sum_rpm, rows_throttle_speed_threshold, total_rows)
    val seqFunc: (TelemetryAcc, (Double, Int, Double, Double, Double)) => TelemetryAcc = {
        case (TelemetryAcc(maxAcc, brakeSum, rpmSum, countHigh, totalRows), (accY, brake, rpm, speed, throttle)) =>

            val newTop = (accY :: maxAcc).sorted.takeRight(5)

            val (newRpmSum, newCountHigh) = if (speed > 120 && throttle > 35) {
                (rpmSum + rpm, countHigh + 1L)
            } else {
                (rpmSum, countHigh)
            }

            TelemetryAcc(
                newTop,
                brakeSum + brake,
                newRpmSum,
                newCountHigh,
                totalRows + 1L)
    }

    // combining function between partitions
    val combFunc: (TelemetryAcc, TelemetryAcc) => TelemetryAcc = {
        case (TelemetryAcc(top1, brake1, rpm1, high1, total1), TelemetryAcc(top2, brake2, rpm2, high2, total2)) =>
            val mergedTop = (top1 ++ top2).sorted.takeRight(5)
            TelemetryAcc(mergedTop,
                brake1 + brake2,
                rpm1 + rpm2,
                high1 + high2,
                total1 + total2)
    }

    // mapFunc - final transformation after aggregation
    val mapFunc: TelemetryAcc => (Double, Double, Double) = {
        case TelemetryAcc(topAcc, brakeSum, rpmSum, countHigh, totalRows) =>
            val p95accY = if (topAcc.nonEmpty) topAcc.sum / topAcc.size else 0.0
            (p95accY,
            brakeSum / totalRows,
            if (countHigh > 0) rpmSum / countHigh else rpmSum)
    }

    val styleSeqFunc: (DrivingStyleAcc, (Double, Double, Double)) => DrivingStyleAcc = {
        case (DrivingStyleAcc(accSum, brakeSum, rpmSum, totalRows), (acc, brk, rpm)) =>
            DrivingStyleAcc (accSum + acc, brakeSum + brk, rpmSum + rpm, totalRows + 1L)
    }

    val styleCombFunc: (DrivingStyleAcc, DrivingStyleAcc) => DrivingStyleAcc = {
        case (DrivingStyleAcc(accSum, brakeSum, rpmSum, totalRows), DrivingStyleAcc(accSum_2, brakeSum_2, rpmSum_2, totalRows_2)) =>
            DrivingStyleAcc (accSum + accSum_2, brakeSum + brakeSum_2, rpmSum + rpmSum_2, totalRows + totalRows_2)
    }

    val styleMapFunc: DrivingStyleAcc => (Double, Double, Double) = {
        case DrivingStyleAcc(accSum, brakeSum, rpmSum, totalRows) =>
            (accSum / totalRows, brakeSum / totalRows, rpmSum / totalRows)
    }
}
