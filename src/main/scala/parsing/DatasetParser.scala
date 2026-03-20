package parsing

import org.apache.spark
import org.apache.spark.sql.{Column, DataFrame, SQLImplicits, SparkSession}
import org.apache.spark.sql.functions

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
        import spark.implicits._
        val dataFrameFlat = dataFrame.select("tel.*")
        dataFrameFlat.prettyPrint("DataFrameFlat: ")

        val columnsNames: Array[String] = dataFrameFlat
            .columns
            .filter(e => e != "dataKey")
        val columns: Array[Column] = columnsNames.map(e => dataFrameFlat(e))

        val zipped = functions.arrays_zip(columns:_*)
        val dataFrameZipped = dataFrameFlat.withColumn("zipped", zipped);
        dataFrameZipped.prettyPrint("dataFrameZipped: ")

        val exploded = functions.inline(dataFrameZipped("zipped"))
        //val columnsMap: Map[String, Column] = dataFrameFlat.columns.zip(exploded).toMap

        val dataFrameExploded = dataFrameZipped.select(exploded.as(columnsNames.toSeq));
        dataFrameExploded.prettyPrint("dataFrameExploded: ")

        dataFrameExploded.write.csv(path)
    }
}
