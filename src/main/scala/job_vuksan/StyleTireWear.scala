package job_vuksan

import job_vuksan.parser.F1DataParsing
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.Commons

object StyleTireWear {
    private val path_to_datasets: String = "/datasets/"

    private val path_ml_telemetry: String = path_to_datasets + "/telemetry/*.csv"
    private val path_ml_laptimes: String = path_to_datasets + "/laptimes/*.csv"

    private val path_output_tireWearOnStyle: String = "/output/tireWearOnStyle"

    def main(args: Array[String]): Unit = {
        if (args.length < 2) {
            println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
            println("The second parameter should indicate the job: "
                + "1 for unoptimized version, "
                + "2 for optimized version")
            return
        }

        val deploymentMode = args(0)
        val job = args(1)
        val writeMode = if (deploymentMode == "sharedRemote") "remote" else deploymentMode

        val spark = SparkSession.builder
            .config(new SparkConf().setAppName("style-tire-wear-job"))
            .getOrCreate()
        val sc = spark.sparkContext
        import spark.sqlContext.implicits._

        // Initializing input datasets
        val rddTelemetry = sc.textFile(Commons.getDatasetPath(deploymentMode, path_ml_telemetry))
        val rddLapTimes = sc.textFile(Commons.getDatasetPath(deploymentMode, path_ml_laptimes))

        val telemetryHeader = rddTelemetry.take(1)(0)
        val lapTimesHeader = rddLapTimes.take(1)(0)

        if (job == "1") {
            // Session type is always "Race" after the filter, so it is excluded from the key
            val rddDrivingStyleParams = rddTelemetry
                .filter(row => row != telemetryHeader)
                .flatMap(F1DataParsing.parseTelemetryRow)
                // if rel_distance is None then all telemetry seems to be invalid, also filtering only for race session type
                .filter { case (_, record) => record.rel_distance != "None" && record.sessionType == "Race" }
                .map { case (key, record) => (key, (record.acc_y, record.brake, record.rpm, record.speed, record.throttle)) }
                .aggregateByKey(AggregationFunctions.TelemetryAcc(List.empty, 0, 0, 0, 0L))(
                    AggregationFunctions.seqFunc,
                    AggregationFunctions.combFunc
                )
                .mapValues(AggregationFunctions.mapFunc)

            // driving style thresholds are inferred from the dataset itself and that is what trackBaselines is used for
            // aggregating only by event, so we can get global thresholds
            val trackBaselines = rddDrivingStyleParams
                .map { case (key, stats) => (key.event, stats) }
                .aggregateByKey(AggregationFunctions.DrivingStyleAcc(0.0, 0.0, 0.0, 0L))(
                    AggregationFunctions.styleSeqFunc,
                    AggregationFunctions.styleCombFunc
                )
                .mapValues(AggregationFunctions.styleMapFunc)

            //  assigning a "style" label to each row based on some parameters that define the style
            //  we assume that above average is considered agressive in at least 2 of three categories such as max lateral
            //  acceleration and high rpms
            //  one category higher than the average is considered balanced, the rest is conservative
            val rddTelemetryWStyle = rddDrivingStyleParams
                .map { case (key, stats) => (key.event, (key, stats)) }
                .join(trackBaselines)
                .map { case (_, ((key, stats), (accMean, brkMean, rpmMean))) =>
                    (key, LabelingFunctions.assignDrivingStyle(stats)(accMean, brkMean, rpmMean))
                }

            // cleaning the laptimes dataset for invalid rows
            val rddLapTimesKV = rddLapTimes
                .filter(row => row != lapTimesHeader)
                .flatMap(F1DataParsing.parseLaptimesRow)
                .filter { case (_, record) =>
                    record.sessionType == "Race" &&
                        record.lapTime != 0.0 &&
                        record.tyreCompound.nonEmpty &&
                        record.tyreAgeLaps != 0
                }

            val dropoffRdd = rddTelemetryWStyle
                .join(rddLapTimesKV)
                .flatMap { case (key, (style, lapRecord)) =>
                    val (_, tyreState) = LabelingFunctions.assignTyreWearFunc((style, lapRecord))
                    // INTERMEDIATE AND WET data seems to have some off-the-charts data, removing them
                    if (tyreState == "DROP"
                        || lapRecord.pitOut != "None"
                        || lapRecord.lap <= 3
                        || lapRecord.tyreCompound == "INTERMEDIATE"
                        || lapRecord.tyreCompound == "WET") None
                    else {
                        // applying tire wear state based on lap number, also mapping lapTimes with fuel correction, making early
                        // laps faster as the car is heavier due to fuel being at its full. In this way the data is not biased by fuel weight.
                        // early laps are slower due to heavier fuel load, subtracting the estimated
                        // penalty normalizes all laps to what they would be on an empty tank
                        val fuelCorrectedTime =
                            lapRecord.lapTime - ((100 * (1 - lapRecord.lap.toDouble / lapRecord.totalLaps.toDouble)) * 0.03)
                        Some(((key.event, style, lapRecord.tyreCompound, tyreState), fuelCorrectedTime))
                    }
                }
                .groupByKey()
                .mapValues { times =>
                    val sorted = times.toArray.sorted
                    val median = sorted(sorted.length / 2)
                    (median, sorted.length)
                }
                .map { case ((event, style, compound, state), (median, count)) =>
                    ((event, style, compound), (state, median, count))
                }
                // median lap time per (event, style, compound, tyreState)
                .groupByKey()
                .flatMap { case ((event, style, compound), items) =>
                    val opt = items.find(_._1 == "OPTIMAL")
                    val deg = items.find(_._1 == "DEGRADED")
                    // Filtering out rows that are missing one of the two states,
                    // otherwise drop-off calculation is meaningless,
                    // considering both acceptable if above 15 lap counts per tire type
                    (opt, deg) match {
                        case (Some((_, optMed, optCnt)), Some((_, degMed, degCnt))) if optCnt >= 15 && degCnt >= 15 =>
                            Some((event, style, compound, optMed, degMed, degMed - optMed, optCnt, degCnt))
                        case _ => None
                    }
                }

            dropoffRdd
                .toDF("event", "style", "tyre_compound", "optimal_median",
                    "degraded_median", "pace_dropoff_s", "optimal_count", "degraded_count")
                .withColumn("optimal_median", round(col("optimal_median"), 3))
                .withColumn("degraded_median", round(col("degraded_median"), 3))
                .withColumn("pace_dropoff_s", round(col("pace_dropoff_s"), 3))
                .coalesce(1)
                .orderBy("event", "style", "tyre_compound")
                .write.format("csv").option("header", "true").mode(SaveMode.Overwrite)
                .save(Commons.getDatasetPath(writeMode, path_output_tireWearOnStyle))

        } else if (job == "2") {
            val reducedNumPartitions = 30; // 2 * total_cores (15 for 5 executors)

            val rddDrivingStyleParams = rddTelemetry
                .filter(row => row != telemetryHeader)
                .flatMap(F1DataParsing.parseTelemetryRow)
                .filter { case (_, record) => record.rel_distance != "None" && record.sessionType == "Race" }
                .map { case (key, record) => (key, (record.acc_y, record.brake, record.rpm, record.speed, record.throttle)) }
                .aggregateByKey(
                    AggregationFunctions.TelemetryAcc(List.empty, 0, 0, 0, 0L),
                    numPartitions = reducedNumPartitions
                )(
                    AggregationFunctions.seqFunc,
                    AggregationFunctions.combFunc
                )
                .mapValues(AggregationFunctions.mapFunc)
                .cache()
            // caching because it's used twice, also repartitioned down from 2000+ partitions

            val trackBaselinesMap = rddDrivingStyleParams
                .map { case (key, stats) => (key.event, stats) }
                .aggregateByKey(AggregationFunctions.DrivingStyleAcc(0.0, 0.0, 0.0, 0L))(
                    AggregationFunctions.styleSeqFunc,
                    AggregationFunctions.styleCombFunc
                )
                .mapValues(AggregationFunctions.styleMapFunc)
                .collectAsMap()

            val bTrackBaselines = sc.broadcast(trackBaselinesMap)

            // Skipping the join (a shuffle) entirely by doing a simple "broadcast join"
            val rddTelemetryWStyle = rddDrivingStyleParams
                .flatMap { case (key, stats) =>
                    bTrackBaselines.value.get(key.event).map { case (accMean, brkMean, rpmMean) =>
                        (key, LabelingFunctions.assignDrivingStyle(stats)(accMean, brkMean, rpmMean))
                    }
                }

            val rddLapTimesKV = rddLapTimes
                .filter(row => row != lapTimesHeader)
                .flatMap(F1DataParsing.parseLaptimesRow)
                .filter { case (_, record) =>
                    record.sessionType == "Race" &&
                        record.lapTime != 0.0 &&
                        record.tyreCompound.nonEmpty &&
                        record.tyreAgeLaps != 0
                }

            val dropoffRdd = rddTelemetryWStyle
                .join(rddLapTimesKV)
                .flatMap { case (key, (style, lapRecord)) =>
                    val (_, tyreState) = LabelingFunctions.assignTyreWearFunc((style, lapRecord))
                    if (tyreState == "DROP"
                        || lapRecord.pitOut != "None"
                        || lapRecord.lap <= 3
                        || lapRecord.tyreCompound == "INTERMEDIATE"
                        || lapRecord.tyreCompound == "WET") None
                    else {
                        val fuelCorrectedTime =
                            lapRecord.lapTime - ((100 * (1 - lapRecord.lap.toDouble / lapRecord.totalLaps.toDouble)) * 0.03)
                        Some(((key.event, style, lapRecord.tyreCompound), (tyreState, fuelCorrectedTime)))
                    }
                }
                // single groupByKey on (event, style, compound), collects all (state, lapTime) pairs together
                .groupByKey()
                .flatMap { case ((event, style, compound), items) =>
                    val optTimes = items.collect { case ("OPTIMAL",  t) => t }.toArray.sorted
                    val degTimes = items.collect { case ("DEGRADED", t) => t }.toArray.sorted

                    if (optTimes.length >= 15 && degTimes.length >= 15) {
                        val optMed = optTimes(optTimes.length / 2)
                        val degMed = degTimes(degTimes.length / 2)
                        Some((event, style, compound, optMed, degMed, degMed - optMed, optTimes.length, degTimes.length))
                    } else None
                }

            dropoffRdd
                .toDF("event", "style", "tyre_compound", "optimal_median",
                    "degraded_median", "pace_dropoff_s", "optimal_count", "degraded_count")
                .withColumn("optimal_median", round(col("optimal_median"), 3))
                .withColumn("degraded_median", round(col("degraded_median"), 3))
                .withColumn("pace_dropoff_s", round(col("pace_dropoff_s"), 3))
                .coalesce(1)
                .orderBy("event", "style", "tyre_compound")
                .write.format("csv").option("header", "true").mode(SaveMode.Overwrite)
                .save(Commons.getDatasetPath(writeMode, path_output_tireWearOnStyle))

        }
        spark.stop()
    }
}