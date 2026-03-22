package parsing

import org.apache.spark.sql.DataFrame

trait Parser {
    def jsonToDataFrame(inPath: String): DataFrame
    def dataFrameToCSV(dataFrame: DataFrame, outPath: String): Unit
    def jsonToCSV(inPath: String, outPath: String): Unit = {
        dataFrameToCSV(jsonToDataFrame(inPath), outPath)
    }

}
