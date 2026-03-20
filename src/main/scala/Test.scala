import TestSimpleRDD.{inputFile, outputDir}
import org.apache.spark.{SparkConf, SparkContext}
import parsing.{DatasetParser, PathWalker}
import utils.Commons
import java.nio.file.Path

object Test {
    val inputDir = "/*/*/*/*_tel.json"
    val outputDir = "/output/csv"

    def main(args: Array[String]): Unit = {
        implicit val sparkSession = Commons.initializeSparkSession("test_telemetry");
        val parser = new DatasetParser();
        val dataFrame = parser.parseTelemetryFile(Commons.getDatasetPath("local", inputDir));
        parser.toCSV(dataFrame, Commons.getDatasetPath("local", outputDir))
    }
}
