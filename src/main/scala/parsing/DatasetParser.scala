package parsing

import org.apache.spark
import org.apache.spark.sql.{Column, DataFrame, SQLImplicits, SparkSession}
import org.apache.spark.sql.functions.{col, split, arrays_zip, inline}

class DatasetParser(implicit spark: SparkSession) {

    implicit class DataFramePrinter(df: DataFrame) {
        def prettyPrint(header: String): Unit = {
            println(header)
            println("Schema: ")
            df.printSchema()
            println("Show: ")
            df.show()
            println("---")
        }
    }

    def parseTelemetryFile(path: String): DataFrame = {
        val dfReader = spark.read;
        // sets reader to treat normal JSON files
        dfReader.option("multiLine", value = true);
        val resultDF = dfReader.json(path);
        resultDF.prettyPrint("schema of " + path +": ")
        resultDF;
    }

    def toCSV(dataFrame: DataFrame, path: String): Unit = {
        val dataFrameFlat = dataFrame.select("tel.*")

        val dfWithKey = dataFrameFlat.withColumn("keyArray", split(col("dataKey"), "-"))

        val columnsNames: Array[String] = dataFrameFlat
            .columns
            .filter(e => e != "dataKey")
        val columns: Array[Column] = columnsNames.map(e => dataFrameFlat(e))

        val zipped = arrays_zip(columns:_*)
        val dataFrameZipped = dfWithKey.withColumn("zipped", zipped);

        val exploded = inline(dataFrameZipped("zipped"))

        val dataFrameExploded = dataFrameZipped.select(
            col("keyArray").getItem(0).as("year"),
            col("keyArray").getItem(1).as("event"),
            col("keyArray").getItem(2).as("sessionType"),
            col("keyArray").getItem(3).as("driverName"),
            col("keyArray").getItem(4).as("lapNumber"),
            exploded.as(columnsNames.toSeq));

        dataFrameExploded.write.csv(path)
    }
}
