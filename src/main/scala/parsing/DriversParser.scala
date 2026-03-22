package parsing

import org.apache.spark.sql.{DataFrame, SparkSession}

class DriversParser (implicit spark: SparkSession) extends Parser {

    override def jsonToDataFrame(inPath: String): DataFrame = ???

    override def dataFrameToCSV(dataFrame: DataFrame, outPath: String): Unit = ???
}
