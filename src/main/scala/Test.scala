import TestSimpleRDD.{inputFile, outputDir}
import org.apache.spark.{SparkConf, SparkContext}
import parsing.DatasetParser
import utils.Commons

object Test {
    val inputFile = "/dataset/Miami_Grand_Prix/Sprint_Qualifying/VER/1_tel.json"
    val outputDir = "/output/csv"

    def main(args: Array[String]): Unit = {
        implicit val sparkSession = Commons.initializeSparkSession("test_telemetry");
        val parser = new DatasetParser();
        val dataFrame = parser.parseTelemetryFile(Commons.getDatasetPath("local", inputFile));
        parser.toCSV(dataFrame, Commons.getDatasetPath("local", outputDir))

    }
}
