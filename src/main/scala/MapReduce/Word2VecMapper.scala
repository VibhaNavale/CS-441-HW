package MapReduce

import scala.jdk.CollectionConverters._
import scala.util.{Try, Success}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import com.github.jfasttext.JFastText
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory

class Word2VecMapper extends Mapper[LongWritable, Text, Text, Text] {
  private val logger = LoggerFactory.getLogger(getClass) // Initialize the logger
  private var model: JFastText = _

  override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    model = new JFastText()

    // Load FastText model from configuration
    model.loadModel("src/main/resources/cc.en.50.bin")
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    val line = value.toString
    val parts = line.split("\\s+")

    if (parts.length >= 2) {
      val cleanedToken = parts(0).replaceAll("[^a-zA-Z0-9]", "").toLowerCase
      val tokenId = parts(1)

      if (cleanedToken.nonEmpty) {
        val vector = Try(model.getVector(cleanedToken).asScala.toArray.map(_.toDouble)) match {
          case Success(vec) =>
            logger.debug(s"Successfully retrieved vector for token: $cleanedToken")
            vec.take(5) // Truncate to 5 dimensions
          case _ =>
            logger.warn(s"Failed to retrieve vector for token: $cleanedToken. Using zero vector.")
            Array.fill(5)(0.0) // Fallback to 5D zero vector
        }
        // Output without brackets around tokenId
        context.write(new Text(s"$cleanedToken $tokenId"), new Text(vector.mkString(",")))
      }
    } else {
      logger.warn(s"Skipping line due to insufficient parts: $line")
    }
  }
}
