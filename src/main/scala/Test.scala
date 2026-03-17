import TestSimpleRDD.inputFile
import org.apache.spark.{SparkConf, SparkContext}
import parsing.DatasetParser
import utils.Commons

object Test {
    val inputFile = "/dataset/Miami_Grand_Prix/Sprint_Qualifying/VER/1_tel.json"

    def main(args: Array[String]): Unit = {
        implicit val sparkSession = Commons.initializeSparkSession("test_telemetry");
        val parser = new DatasetParser();
        parser.parseFile(Commons.getDatasetPath("local", inputFile));
    }
}
