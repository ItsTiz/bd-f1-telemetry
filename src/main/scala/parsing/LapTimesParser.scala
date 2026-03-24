package parsing

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{arrays_zip, col, inline, input_file_name, split, element_at, url_decode}
import DataFrameExtensions._
class LapTimesParser(implicit spark: SparkSession) extends Parser {
    private val columnDictionary = Map(
        "drv"  -> "driverCode",
        "dNum" -> "driverNumber",
        "time" -> "lapTime",
        "pb"   -> "isPersonalBest",
        "pos"  -> "position",
        "compound" -> "tyreCompound",
        "life" -> "tyreAgeLaps",
        "stint" -> "stintNumber",
        "s1"   -> "sector1Time",
        "s2"   -> "sector2Time",
        "s3"   -> "sector3Time",
        "vi1"  -> "speedTrapIntermediate1",
        "vi2"  -> "speedTrapIntermediate2",
        "vfl"  -> "speedFinishLine",
        "vst"  -> "speedStraight",
        "wAT"  -> "airTemperature",
        "wH"   -> "humidity",
        "wP"   -> "atmPressure",
        "wR"   -> "isRaining",
        "wTT"  -> "trackTemperature",
        "wWD"  -> "windDirection",
        "wWS"  -> "windSpeed"
    )

    private val selectedColumnNames = List(
        "team", "drv", "dNum", "lap", "time", "pb", "pos",
        "compound", "life", "stint",
        "s1", "s2", "s3", "vi1", "vi2", "vfl", "vst",
        "wAT", "wH",  "wP", "wR", "wTT", "wWD", "wWS"
    )
    override def jsonToDataFrame(inPath: String, multilineValue: Boolean): DataFrame = {
        val dfReader = spark.read;
        dfReader.option("multiLine", multilineValue);
        val resultDF = dfReader.json(inPath);
        val dfWithPath = resultDF.withColumn("full_file_uri", split(input_file_name(), "/"))
        dfWithPath.prettyPrint("schema laptimes: ")
        dfWithPath
    }

    override def dataFrameToCSV(dataFrame: DataFrame, outPath: String): Unit = {
        val validColumnsNames = selectedColumnNames.filter(name => dataFrame.columns.contains(name))

        val columns: List[Column] = validColumnsNames.map { oldName =>
            val newDescriptiveName = columnDictionary.getOrElse(oldName, oldName)
            col(oldName).alias(newDescriptiveName)
        }

        val zipped = arrays_zip(columns: _*)
        val dataFrameZipped = dataFrame.withColumn("zipped", zipped)

        val uriColumn = col("full_file_uri")

        val dataFrameExploded = dataFrameZipped.select(
            url_decode(element_at(uriColumn, -4)).as("event"),
            url_decode(element_at(uriColumn, -3)).as("sessionType"),
            inline(col("zipped"))
        )

        val finalCleanedDf = dataFrameExploded
            .withColumn("lap", col("lap").cast("int"))
            .withColumn("stintNumber", col("stintNumber").cast("int"))
            .withColumn("tyreAgeLaps", col("tyreAgeLaps").cast("int"))

        finalCleanedDf
            .write
            .option("header", "true")
            .csv(outPath)

    }
}
