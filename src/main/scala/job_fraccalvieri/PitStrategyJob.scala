package job_fraccalvieri

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.Commons
import job_fraccalvieri.AggregationFunctions._
import job_fraccalvieri.LabelingFunctions.assignStrategy
import job_fraccalvieri.parser.F1DataParsing

object PitStrategyJob {

  private val path_to_datasets: String = "/datasets/"
  private val path_ml_laptimes: String = path_to_datasets + "/laptimes/*.csv"
  private val path_ml_drivers: String = path_to_datasets + "/drivers/*.csv"
  private val path_output: String = "/output/pitStrategy"

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Specify deployment mode (local or remote)")
      return
    }

    val deploymentMode = args(0)
    var writeMode = deploymentMode
    if (deploymentMode == "sharedRemote") writeMode = "remote"

    val sparkConf = new SparkConf()
      .setAppName("pit-strategy-job-final")
      .set("spark.executor.instances", "3")
      .set("spark.executor.cores", "1")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "2g")
      .set("spark.sql.shuffle.partitions", "12")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    // LOAD DATA

    val rddLapTimes = sc.textFile(
      Commons.getDatasetPath(deploymentMode, path_ml_laptimes)
    )

    val rddDrivers = sc.textFile(
      Commons.getDatasetPath(deploymentMode, path_ml_drivers)
    )

    val headerLap = rddLapTimes.take(1)
    val headerDrivers = rddDrivers.take(1)

    // PARSE LAPTIMES

    val lapRDD = rddLapTimes
      .filter(_ != headerLap(0))
      .flatMap(F1DataParsing.parseLaptimesRow)
      .filter(row =>
        row._1.sessionType == "Race" &&
          row._2.lapTime > 0 &&
          row._2.tyreCompound.nonEmpty
      )

    // DRIVERS (JOIN SU driverCode)

    val driversKV = rddDrivers
      .filter(_ != headerDrivers(0))
      .map(line => {
        val cols = line.split(",")

        val driverCode = cols(0)
        val team = cols(1)
        val driverNumber = cols(2)

        (driverCode, (team, driverNumber))
      })

    // COSTRUZIONE RISULTATI GARA

    val resultsRDD = lapRDD
      .map { case (key, lap) =>
        ((key.event, key.driverCode), (lap.lap, lap.lapTime))
      }
      .groupByKey()
      .mapValues { laps =>
        val ordered = laps.toList.sortBy(_._1)
        val totalTime = ordered.map(_._2).sum
        val finalLap = ordered.last._1
        (finalLap, totalTime)
      }
      .filter { case (_, (_, totalTime)) =>
        totalTime > 0   // filtro DNF
      }

    // CLASSIFICA POSIZIONI

    val rankedRDD = resultsRDD
      .map { case ((event, driver), (_, totalTime)) =>
        (event, (driver, totalTime))
      }
      .groupByKey()
      .flatMap { case (event, drivers) =>
        drivers.toList
          .sortBy(_._2)
          .zipWithIndex
          .map { case ((driver, time), idx) =>
            ((event, driver), (idx + 1, time))
          }
      }

    // PREPARAZIONE DATI STINT

    val lapKeyed = lapRDD.map {
      case (key, lap) =>
        ((key.event, key.driverCode), lap)
    }

    val joined = lapKeyed.join(rankedRDD)

    val prepared = joined.map {
      case ((event, driver), (lap, (pos, totalTime))) =>
        (
          (event, driver),
          (lap.stintNumber, lap.lapTime, lap.tyreCompound)
        )
    }

    // AGGREGAZIONE STRATEGIA

    val aggregated = prepared
      .aggregateByKey(StrategyAcc(Map(), 0.0, 0.0, 0))(
        seqFunc,
        combFunc
      )
      .mapValues(mapFunc)

    // CLASSIFICAZIONE STRATEGIA

    val classified = aggregated.map {
      case ((event, driver), (_, _, _, avgStint, compounds)) =>
        val strategy = assignStrategy(compounds, avgStint)
        ((event, driver), strategy)
    }

    // AGGIUNTA PERFORMANCE

    val withPerformance = classified.join(rankedRDD)

    val enriched = withPerformance
      .map { case ((event, driver), (strategy, (pos, totalTime))) =>
        (driver, (event, strategy, pos, totalTime))
      }
      .join(driversKV)
      .map {
        case (driver, ((event, strategy, pos, totalTime), (team, number))) =>
          ((strategy, event, team), (pos, totalTime, 1))
      }

    // AGGREGAZIONE FINALE

    val finalRDD = enriched
      .reduceByKey { case ((p1, t1, c1), (p2, t2, c2)) =>
        (p1 + p2, t1 + t2, c1 + c2)
      }
      .mapValues { case (p, t, c) =>
        (p.toDouble / c, t / c)
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