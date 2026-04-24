package job_vuksan

import job_vuksan.parser.F1DataParsing
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession, SQLContext}
import utils.Commons
import AggregationFunctions.TelemetryAcc
import utils.Commons._

object StyleTireWear {
    private val path_to_datasets: String = "/datasets/"

    private val path_ml_telemetry: String = path_to_datasets + "/telemetry/*.csv"
    private val path_ml_laptimes: String = path_to_datasets + "/laptimes/*.csv"
    private val path_ml_drivers: String = path_to_datasets + "/drivers/*.csv"

    private val path_output_tireWearOnStyle: String = "/output/tireWearOnStyle"

    def main(args: Array[String]): Unit = {
        if (args.length < 1) {
            println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
            return
        }
        val deploymentMode = args(0)
        var writeMode = deploymentMode
        if (deploymentMode == "sharedRemote") {
            writeMode = "remote"
        }

        val sparkConf = new SparkConf()
            .setAppName("style-tire-wear-job")
        val spark = SparkSession.builder.config(sparkConf).getOrCreate()
        val sqlContext = spark.sqlContext // needed to save as CSV
        val sparkContext: SparkContext = spark.sparkContext
        import sqlContext.implicits._

        // Initializing input datasets
        val rddTelemetry = sparkContext.textFile(Commons.getDatasetPath(deploymentMode, path_ml_telemetry))
        val rddLapTimes = sparkContext.textFile(Commons.getDatasetPath(deploymentMode, path_ml_laptimes))

        println(s"Number of partitions for telemetry dataset: ${rddTelemetry.getNumPartitions}")
        println(s"Number of partitions for laptimes dataset: ${rddLapTimes.getNumPartitions}")

        val columnNames = rddTelemetry.take(1)
        val columnNamesLaptimes = rddLapTimes.take(1)

        val rddTelemetryKV = rddTelemetry
            .filter(row => row != columnNames(0)) //skip the header column names
            .flatMap(F1DataParsing.parseTelemetryRow)
            .filter(row => row._2.rel_distance != "None" && row._1.sessionType.equals("Race")) // if rel_distance is None then all telemetry seems to be invalid
            .map(row => (
                (row._1.event, row._1.driveCodeName),
                (row._2.acc_y, row._2.brake, row._2.rpm, row._2.speed, row._2.throttle)
            ))

        val rddDrivingStyleParams = rddTelemetryKV
            .aggregateByKey(AggregationFunctions.TelemetryAcc(List.empty, 0, 0, 0, 0))(
                AggregationFunctions.seqFunc, AggregationFunctions.combFunc)
            .mapValues(AggregationFunctions.mapFunc)

        //  assigning a "style" label to each row based on some parameters that define the style
        //  we assume that above average is considered agressive in at least 2 of three categories such as max lateral
        //  acceleration and high rpms
        //  one category higher than the average is considered balanced, the rest is conservative
        //  as averages we take 30 m/s^2 as the high threshold, braking percentage 20% and rpms around 10500

        // Note: Maybe these should be grouped by TrackID rather than globally, because it depends on track
        //        val acc_y_mean_threshold = rddDrivingStyleParams.map(row => row._2._1).mean()
        //        val brake_mean_threshold = rddDrivingStyleParams.map(row => row._2._2).mean()
        //        val rpms_mean_threshold = rddDrivingStyleParams.map(row => row._2._3).mean()

        //        val rddTelemetryWStyle = rddDrivingStyleParams
        //            .mapValues(
        //                line => LabelingFunctions.assignDrivingStyle(line)
        //                (
        //                    acc_y_mean_threshold,
        //                    brake_mean_threshold,
        //                    rpms_mean_threshold
        //                ))

        val trackBaselines = rddDrivingStyleParams
            .map { case ((event, _), stats) => (event, stats) }
            .aggregateByKey((0.0, 0.0, 0.0, 0L))(
                { case ((sA, sB, sR, count), (acc, brk, rpm)) =>
                    (sA + acc, sB + brk, sR + rpm, count + 1L)
                },
                { case ((sA1, sB1, sR1, c1), (sA2, sB2, sR2, c2)) =>
                    (sA1 + sA2, sB1 + sB2, sR1 + sR2, c1 + c2)
                }
            )
            .mapValues { case (sumAcc, sumBrk, sumRpm, count) =>
                (sumAcc / count, sumBrk / count, sumRpm / count)
            }

        val rddTelemetryWStyle = rddDrivingStyleParams
            .map { case ((event, driver), stats) => (event, ((event, driver), stats)) }
            .join(trackBaselines)
            .map { case (event, (((event_dup, driver), stats), (trackAccMean, trackBrkMean, trackRpmMean))) =>
                val style = LabelingFunctions.assignDrivingStyle(stats)(trackAccMean, trackBrkMean, trackRpmMean)
                ((event, driver), style)
            }

        val rddLapTimesKV = rddLapTimes
            .filter(row => row != columnNamesLaptimes(0))
            .flatMap(F1DataParsing.parseLaptimesRow)
            .filter(row => row._1.sessionType.equals("Race") && row._2.lapTime != 0.0 && row._2.tyreCompound.nonEmpty && row._2.tyreAgeLaps != 0)
            .map(row => ((row._1.event, row._1.driveCodeName), row._2))

        val joinedRDD = rddTelemetryWStyle.join(rddLapTimesKV)

        val joinedRddLabeled = joinedRDD
            .mapValues(line => LabelingFunctions.assignTyreWearFunc(line))
            .filter(row => row._2._2 != "DROP")
            .mapValues(row => (
                row._1._1,
                row._1._2.tyreCompound,
                row._2,
                row._1._2.lapTime - ((100 * (1 - row._1._2.lap.toDouble / row._1._2.totalLaps.toDouble)) * 0.03),
                row._1._2.lap,
                row._1._2.totalLaps,
                row._1._2.pitOut, 1)
            )
            //INTERMEDIATE AND WET data seems to have some off-the-charts data, removing them
            .filter(row => row._2._7.equals("None") && row._2._5 > 3 && row._2._2 != "INTERMEDIATE" && row._2._2 != "WET")

        val finalRdd = joinedRddLabeled
            .map(row => ((row._1._1, row._2._1, row._2._2, row._2._3), row._2._4))
            .groupByKey()
            .mapValues { times =>
                val sortedTimes = times.toArray.sorted
                val count = sortedTimes.length
                val median = if (count > 0) sortedTimes(count / 2) else 0.0
                val mean = if (count > 0) sortedTimes.sum / count else 0.0

                (median, count, mean)
            }

        val dropoffRdd = finalRdd
            // Shifting state out of the key and into the value part
            .map { case ((event, style, compound, state), (median, count, mean)) =>
                ((event, style, compound), (state, median, count))
            }
            .groupByKey()
            .mapValues { items =>
                val opt = items.find(_._1 == "OPTIMAL")
                val deg = items.find(_._1 == "DEGRADED")

                val optMed = opt.map(_._2).getOrElse(0.0)
                val degMed = deg.map(_._2).getOrElse(0.0)
                val optCnt = opt.map(_._3).getOrElse(0)
                val degCnt = deg.map(_._3).getOrElse(0)

                val dropoff = if (opt.isDefined && deg.isDefined) degMed - optMed else 0.0

                (optMed, degMed, dropoff, optCnt, degCnt)
            }

        dropoffRdd
            .map { case ((event, style, compound), (optMed, degMed, dropoff, optCnt, degCnt)) =>
                // Helper function to round to 3 decimal places for a clean CSV
                def round3(v: Double): Double = BigDecimal(v).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble

                (
                    event.toString,
                    style,
                    compound,
                    round3(optMed),
                    round3(degMed),
                    round3(dropoff),
                    optCnt,
                    degCnt
                )
            }
            // Filtering out rows that are missing one of the two states,
            // otherwise drop-off calculation is meaningless, cosnidering both acceptable if above 15 laps per tyre type
            .filter(row => row._7 >= 15 && row._8 >= 15)
            .coalesce(1)
            .toDF(
                "event",
                "style",
                "tyre_compound",
                "optimal_median",
                "degraded_median",
                "pace_dropoff_s",
                "optimal_count",
                "degraded_count"
            ).write.format("csv").option("header", "true").mode(SaveMode.Overwrite)
            .save(Commons.getDatasetPath(writeMode, path_output_tireWearOnStyle))

        spark.stop()
    }
}