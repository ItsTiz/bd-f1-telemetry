package parsing

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import parsing.DataFrameExtensions._
import org.apache.spark.sql.functions.{col, inline}

class DriversParser (implicit spark: SparkSession) extends Parser {

    override def dataFrameToCSV(dataFrame: DataFrame, outPath: String): Unit = {
        val inlinedDf = dataFrame.select(inline(dataFrame("drivers")))

        val filteredDf = inlinedDf.select(
            col("driver").alias("driverName"),
            col("team"),
            col("dn").alias("driverNumber"),
            col("fn").alias("firstName"),
            col("ln").alias("lastName")
        )

        filteredDf.prettyPrint("drivers: ")

        filteredDf
            .write
            .option("header", "true")
            .csv(outPath)
    }
}
