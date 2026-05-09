package parsing

import org.apache.spark.sql.DataFrame

object DataFrameExtensions {
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
}


