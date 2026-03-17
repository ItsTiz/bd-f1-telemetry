package parsing

import org.apache.spark
import org.apache.spark.sql.{SparkSession, DataFrame, SQLImplicits}

class DatasetParser(implicit spark: SparkSession) {
    def parseFile(path: String): DataFrame = {
        val dfReader = spark.read;
        dfReader.option("multiLine", value = true);
        val resultDF = dfReader.json(path);
        println("schema of " + path +": ")
        resultDF.printSchema()
        println("---")
        resultDF;
    }
}
