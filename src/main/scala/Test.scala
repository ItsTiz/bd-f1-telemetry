import org.apache.spark.sql.SparkSession
import parsing.DatasetParser
import utils.Commons

object Test {
    private val inputDir = "/*/*/*/*_tel.json"
    private val outputDir = "/output/csv"
    private val fullInputDir = Commons.getDatasetPath("local", inputDir)
    private val fullOutputDir = Commons.getDatasetPath("local", outputDir)

    def main(args: Array[String]): Unit = {
        implicit val sparkSession: SparkSession = Commons.initializeSparkSession("test_telemetry");
        val parser = new DatasetParser();
        parser.jsonToCSV(fullInputDir, fullOutputDir);
    }
}
