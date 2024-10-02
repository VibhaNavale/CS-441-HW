package MapReduce

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import scala.jdk.CollectionConverters._
import org.slf4j.LoggerFactory

class TokenizedReducer extends Reducer[Text, Text, Text, Text] {
  private val logger = LoggerFactory.getLogger(getClass)

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    // Map for counting occurrences of token IDs
    val tokenIDMap = scala.collection.mutable.Map[Int, Int]()
    val uniqueTokenIDs = scala.collection.mutable.Set[Int]()

    // Process each value emitted by the mapper
    values.asScala.foreach { value =>
      val parts = value.toString.split(" ")
      if (parts.length >= 2) {
        val idsString = parts(0) // The token IDs are here
        val count = parts(1).toInt // Get count

        // Add token IDs to the set and map, ensuring no duplicates
        idsString.stripPrefix("[").stripSuffix("]").split(",").foreach { id =>
          val trimmedID = id.trim // Remove any spaces
          try {
            if (trimmedID.nonEmpty) { // Ensure the ID is not empty
              val tokenID = trimmedID.toInt
              uniqueTokenIDs.add(tokenID) // Add token ID to the Set (avoids duplicates)
              tokenIDMap(tokenID) = tokenIDMap.getOrElse(tokenID, 0) + count
            }
          } catch {
            case _: NumberFormatException =>
              logger.warn(s"Skipping invalid token ID: $trimmedID")
            case _: Exception =>
              logger.error(s"Unexpected error while processing token ID: $trimmedID")
          }
        }
      } else {
        logger.warn(s"Unexpected value format: ${value.toString}")
      }
    }

    // Prepare the output format, now with unique token IDs
    val finalIDs = uniqueTokenIDs.toList.sorted // Sorting for consistency
    val finalFrequency = tokenIDMap.values.sum

    // Emit the final result in the expected format
    context.write(key, new Text(s"[${finalIDs.mkString(", ")}] $finalFrequency"))
  }
}
