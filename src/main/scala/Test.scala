import org.apache.spark.sql.SparkSession
import parsing.{DatasetParser, DriversParser}
import utils.Commons
import utils.Commons._

object Test {
    private val drvInputDir = "/Miami Grand Prix/Practice 1/drivers.json"
    private val drvOutputDir = "/output/drivers"
    private val fullDrvInputDir = drvInputDir.toFullLocalPath
    private val fullDrvOutputDir = drvOutputDir.toFullLocalPath

    private val dsInputDir = "/*/*/*/*_tel.json"
    private val dsOutputDir = "/output/csv"
    private val fullDSInputDir = dsInputDir.toFullLocalPath
    private val fullDSOutputDir = dsOutputDir.toFullLocalPath

    def main(args: Array[String]): Unit = {
        implicit val sparkSession: SparkSession = Commons.initializeSparkSession("test_telemetry");
        val parser = new DriversParser();
        //parser.jsonToDataFrame(fullDrvInputDir)
        parser.jsonToCSV(fullDrvInputDir, fullDrvOutputDir);
    }
}
