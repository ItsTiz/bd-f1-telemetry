package job_fraccalvieri

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.Commons
import job_fraccalvieri.AggregationFunctions._
import job_fraccalvieri.LabelingFunctions.assignStrategy
import job_fraccalvieri.parser.F1DataParsing

object PitStrategyJobOptimizedV2 {

  private val path_to_datasets = "/datasets/"
  private val path_ml_laptimes = path_to_datasets + "/laptimes/*.csv"
  private val path_ml_drivers = path_to_datasets + "/drivers/*.csv"
  private val path_output = "/output/pitStrategy"

  def main(args: Array[String]): Unit = {

    val deploymentMode = args(0)
    val writeMode = if (deploymentMode == "sharedRemote") "remote" else deploymentMode

    val spark = SparkSession.builder()
      .config(new SparkConf()
        .setAppName("pit-strategy-optimized-v2")
        .set("spark.sql.shuffle.partitions", "12"))
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val lapRDD = sc.textFile(Commons.getDatasetPath(deploymentMode, path_ml_laptimes))
      .mapPartitionsWithIndex((idx, it) => if (idx == 0) it.drop(1) else it)
      .flatMap(F1DataParsing.parseLaptimesRow)
      .filter(row =>
          row._1.sessionType == "Race" &&
          row._2.lapTime > 0 &&
          row._2.tyreCompound.nonEmpty
      )

    // DRIVERS (BROADCAST)

    val driversMap = sc.textFile(Commons.getDatasetPath(deploymentMode, path_ml_drivers))
      .mapPartitionsWithIndex((idx, it) => if (idx == 0) it.drop(1) else it)
      .map(line => {
        val c = line.split(",")
        (c(0), (c(1), c(2)))
      })
      .collectAsMap()

    val broadcastDrivers = sc.broadcast(driversMap)

    //AGGREGAZIONE COMPLETA

    val aggregated = lapRDD
      .map { case (key, lap) =>
        ((key.event, key.driverCode),
          (lap.stintNumber, lap.lapTime, lap.tyreCompound, lap.lap))
      }
      .aggregateByKey(StrategyAcc(Map(), 0.0, 0.0, 0))(
        (acc, v) => seqFunc(acc, (v._1, v._2, v._3)),
        combFunc
      )
      .mapValues { acc =>
        val (numStints, avg, varLap, avgStint, compounds) = mapFunc(acc)
        val totalTime = acc.totalLapTime
        (numStints, avgStint, compounds, totalTime)
      }

    // PREPARO STRATEGIA + TEMPO

    val strategyRDD = aggregated.map {
      case ((event, driver), (numStints, avgStint, compounds, totalTime)) =>
        val strategy = assignStrategy(compounds, avgStint)
        (event, (driver, strategy, totalTime))
    }

    // SHUFFLE 2: RANKING

    val ranked = strategyRDD
      .groupByKey()
      .flatMap { case (event, drivers) =>
        drivers.toList
          .sortBy(_._3)
          .zipWithIndex
          .map { case ((driver, strategy, time), idx) =>
            val (team, _) = broadcastDrivers.value.getOrElse(driver, ("UNK", "0"))
            ((strategy, event, team), (idx + 1, time, 1))
          }
      }

    // AGGREGAZIONE FINALE

    val finalRDD = ranked
      .reduceByKey {
        case ((posSum1, timeSum1, count1), (posSum2, timeSum2, count2)) =>
          (
            posSum1 + posSum2,
            timeSum1 + timeSum2,
            count1 + count2
          )
      }
      .mapValues {
        case (posSum, timeSum, count) =>
          val avgPos = posSum.toDouble / count
          val avgTime = timeSum / count
          (avgPos, avgTime)
      }

    // OUTPUT

    finalRDD
      .map { case ((strategy, event, team), (avgPos, avgTime)) =>
        (strategy, event, team, avgPos, avgTime)
      }
      .toDF("strategy", "event", "team", "avgPosition", "avgTotalTime")
      .coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(Commons.getDatasetPath(writeMode, path_output))
  }
}