package parsing

import org.apache.spark.sql.{DataFrame, SparkSession}
import DataFrameExtensions._

abstract class Parser (implicit spark: SparkSession) {
    def jsonToDataFrame(inPath: String, multilineValue: Boolean = true): DataFrame = {
        val dfReader = spark.read;
        dfReader.option("multiLine", multilineValue);
        val resultDF = dfReader.json(inPath);
        resultDF.prettyPrint("schema of " + inPath +": ")
        resultDF;
    }
    def dataFrameToCSV(dataFrame: DataFrame, outPath: String): Unit
    def jsonToCSV(inPath: String, outPath: String): Unit = {
        dataFrameToCSV(jsonToDataFrame(inPath), outPath)
    }
}
