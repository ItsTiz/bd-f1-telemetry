package parsing

import java.nio.file.{Files, Path}
import java.util.stream.Collectors
import scala.collection.JavaConverters.asScalaBufferConverter

object PathWalker {

    case class DataKey(year: String, event: String, sessionType: String, driver: String, lapNumber: Int)

    def listAllFiles(rootFolder: Path): Unit = {
        val files = Files.walk(rootFolder)
            .filter(Files.isRegularFile(_))
            .collect(Collectors.toList[Path])
            .asScala
        files.foreach(println)
    }


}
