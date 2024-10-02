package MapReduce

import scala.jdk.CollectionConverters._
import scala.util.{Try, Success, Failure}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

class Word2VecReducer extends Reducer[Text, Text, Text, Text] {
  // Initialize logger
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val vectorStrings = values.asScala.toList

    val vectors: List[Array[Double]] = vectorStrings.map { value =>
      Try(value.toString.split(",").map(_.toDouble)) match {
        case Success(vector) =>
          vector
        case Failure(ex) =>
          logger.error(s"Error parsing vector: ${value.toString}, error: ${ex.getMessage}")
          Array.fill(5)(0.0) // Fallback to 5D zero vector
      }
    }

    val meanVector = if (vectors.nonEmpty) {
      val mean = Array.fill(5)(0.0)

      vectors.foreach { vector =>
        for (i <- vector.indices) {
          mean(i) += vector(i)
        }
      }

      mean.map(_ / vectors.size)
    } else {
      logger.warn("No vectors received for key: " + key.toString)
      Array.fill(5)(0.0) // Fallback to 5D zero vector
    }

    // Split the key and format it without brackets around tokenId
    val keyParts = key.toString.split(" ")
    val tokenId = keyParts(1).replaceAll("[\\[\\],]", "").trim
    val formattedKey = s"${keyParts(0)} $tokenId"

    // Write the output without brackets around the tokenId
    context.write(new Text(formattedKey), new Text(meanVector.mkString(",")))
  }
}
