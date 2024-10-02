package MapReduce

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.Mapper
import java.io.IOException
import com.knuddels.jtokkit.api.{Encoding, EncodingRegistry, EncodingType}
import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.IntArrayList
import org.slf4j.LoggerFactory

class TokenizedMapper extends Mapper[LongWritable, Text, Text, Text] {
  private val logger = LoggerFactory.getLogger(getClass)
  private val encodingRegistry: EncodingRegistry = Encodings.newDefaultEncodingRegistry()
  private val encoding: Encoding = encodingRegistry.getEncoding(EncodingType.CL100K_BASE)

  @throws[IOException]
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {

    // Cleaning input data:
    // Get the line and remove <br /> tags
    var line = value.toString.replaceAll("<br />", " ")

    // Remove unwanted characters (like emojis and unknown characters)
    line = removeUnwantedCharacters(line)

    // Remove punctuation at the start and end of words, except for hyphens and apostrophes
    line = line.replaceAll("""(?<![\p{L}])[\p{Punct}&&[^'\-]]+|[\p{Punct}&&[^'\-]]+(?![\p{L}])""", "")

    // Replace internal punctuation (e.g., comma or exclamation in between words) with space,
    // while keeping hyphens and apostrophes intact.
    line = line.replaceAll("""[\p{Punct}&&[^'\-]]""", " ")

    // Handle multiple spaces
    line = line.replaceAll("\\s+", " ").trim() // Normalize spaces

    // Tokenize
    val tokens: IntArrayList = encoding.encode(line)

    // Use a map to store token counts and their corresponding token IDs
    val tokenCountMap = scala.collection.mutable.Map[String, (List[Int], Int)]()

    // Process each token with their respective index
    val words = line.split(" ").filter(_.nonEmpty) // Split the line by spaces to get words, remove empty strings
    // logger.debug(s"Words extracted: ${words.mkString(", ")}")

    for ((word, i) <- words.zipWithIndex) {
      val tokenID = tokens.get(i) // This should represent the token ID

      // Check if the word is already in the map
      val (ids, count) = tokenCountMap.getOrElse(word, (List[Int](), 0)) // Get existing ids and count
      tokenCountMap(word) = (tokenID :: ids, count + 1) // Update token IDs and count
      // logger.debug(s"Processed word: '$word', Token ID: $tokenID, Count: ${count + 1}")
    }

    // Emit tokens in the desired output format
    tokenCountMap.foreach { case (word, (ids, count)) =>
      val outputValue = s"[${ids.mkString(",")}] $count"
      context.write(new Text(word), new Text(outputValue))
    }
  }

  // Method to remove unwanted characters (like emojis)
  private def removeUnwantedCharacters(line: String): String = {
    // This regex matches non-ASCII characters (e.g., emojis, special symbols)
    line.replaceAll("[^\\x20-\\x7E]", "")
  }
}
