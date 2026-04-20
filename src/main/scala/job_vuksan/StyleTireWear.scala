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

        // Create a SparkConf object;
        // the configuration settings you put here will override those given in the Run/Debug configuration
        val sparkConf = new SparkConf()
            .setAppName("style-tire-wear-job")

            // === Best settings for m4.large (2 vCPU, 8GB per node) ===
            .set("spark.executor.instances", "3")           // 1 per node + 1 extra
            .set("spark.executor.cores", "1")               // MUST be 1
            .set("spark.executor.memory", "4g")
            .set("spark.executor.memoryOverhead", "800m")   // Important

            .set("spark.driver.memory", "2g")
            .set("spark.driver.cores", "1")

            // Performance
            .set("spark.default.parallelism", "12")
            .set("spark.sql.shuffle.partitions", "12")
            .set("spark.dynamicAllocation.enabled", "false")
        val spark = SparkSession.builder.config(sparkConf).getOrCreate()
        val sqlContext = spark.sqlContext // needed to save as CSV
        val sparkContext: SparkContext = spark.sparkContext
        import sqlContext.implicits._

        // Initialize input datasets
        val rddTelemetry = sparkContext.textFile(Commons.getDatasetPath(deploymentMode, path_ml_telemetry))
        val rddLapTimes = sparkContext.textFile(Commons.getDatasetPath(deploymentMode, path_ml_laptimes))

        println(s"Number of partitions for telemetry dataset: ${rddTelemetry.getNumPartitions}")
        println(s"Number of partitions for laptimes dataset: ${rddLapTimes.getNumPartitions}")

        val columnNames = rddTelemetry.take(1)
        val columnNamesLaptimes = rddLapTimes.take(1)

        val rddTelemetryKV = rddTelemetry
            .filter(row => row != columnNames(0)) //skip the header column names
            .flatMap(F1DataParsing.parseTelemetryRow)
            .filter(row => row._2.rel_distance != "None") // if rel_distance is None then all telemetry seems to be invalid
            .map(row => (row._1, (row._2.acc_y, row._2.brake, row._2.rpm, row._2.speed, row._2.throttle)))

        val rddDrivingStyleParams = rddTelemetryKV
            .aggregateByKey(AggregationFunctions.TelemetryAcc(-30.0, 0, 0, 0, 0))(
                AggregationFunctions.seqFunc, AggregationFunctions.combFunc)
            .mapValues(AggregationFunctions.mapFunc)

        //  assigning a "style" label to each row based on some parameters that define the style
        //  we assume that above average is considered agressive in at least 2 of three categories such as max lateral
        //  acceleration and high rpms
        //  one category higher than the average is considered balanced, the rest is conservative
        //  as averages we take 30 m/s^2 as the high threshold, braking percentage 20% and rpms around 10500
        val acc_y_mean_threshold = rddDrivingStyleParams.map(row => row._2._1).mean()
        val brake_mean_threshold = rddDrivingStyleParams.map(row => row._2._2).mean()
        val rpms_mean_threshold = rddDrivingStyleParams.map(row => row._2._3).mean()

        val rddTelemetryWStyle = rddDrivingStyleParams
            .mapValues(
                line => LabelingFunctions.assignDrivingStyle(line)
                (
                    acc_y_mean_threshold,
                    brake_mean_threshold,
                    rpms_mean_threshold
                ))

        val rddLapTimesKV = rddLapTimes
            .filter(row => row != columnNamesLaptimes(0))
            .flatMap(F1DataParsing.parseLaptimesRow)
            .filter(row => row._2.lapTime != 0.0 && !row._2.tyreCompound.isEmpty && row._2.tyreAgeLaps != 0)

        val joinedRDD = rddTelemetryWStyle.join(rddLapTimesKV)

        val joinedRddLabeled = joinedRDD
            .mapValues(line => LabelingFunctions.assignTyreWearFunc(line))
            .filter(row => row._2._2 != "DROP")
            .mapValues(row => (
                row._1._1,
                row._1._2.tyreCompound,
                row._2,
                row._1._2.lapTime + ((100 * (1 - row._1._2.lap / row._1._2.totalLaps)) * 0.03),
                row._1._2.lap,
                row._1._2.totalLaps,
                row._1._2.pitOut, 1)
            )
            .filter(row => row._1.sessionType.equals("Race") && row._2._7.equals("None") && row._2._5 > 3)

        val finalRdd = joinedRddLabeled
            .map(row => ((row._2._1, row._2._2, row._2._3), row._2._4)) //new key
            .groupByKey() // Group all lap times by (style, compound, state)
            .mapValues { times => // times: Iterable[Double]
                val sortedTimes = times.toArray.sorted
                val count = sortedTimes.length
                val median = if (count > 0) sortedTimes(count / 2) else 0.0
                val mean = if (count > 0) sortedTimes.sum / count else 0.0

                (median, count, mean)
            }

        finalRdd
            .coalesce(1)
            .toDF().write.format("csv").mode(SaveMode.Overwrite)
            .save(Commons.getDatasetPath(writeMode, path_output_tireWearOnStyle))
    }
}