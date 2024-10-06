package MapReduce

import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import com.github.jfasttext.JFastText
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import java.net.URI

class Word2VecMapper extends Mapper[LongWritable, Text, Text, Text] {
  private val logger = LoggerFactory.getLogger(getClass) // Initialize the logger
  private var model: JFastText = _

  override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    model = new JFastText()

    val configuration = new Configuration()

    // Get FileSystem instance
    val fs = FileSystem.get(new URI("s3a://cs441-assignment1/"), configuration)

    // Retrieve model path from the configuration (default to S3)
    val modelPath = context.getConfiguration.get("model.path", "s3a://cs441-assignment1/input/cc.en.50.bin")
    val modelFilePath = new Path(modelPath)

    // Check if the model file exists
    if (fs.exists(modelFilePath)) {
      logger.info(s"Model file exists at path: $modelPath")

      // Load the model directly from S3
      model.loadModel(modelPath)
    } else {
      logger.error(s"Model file does not exist at path: $modelPath")
      throw new IllegalArgumentException(s"Model file does not exist at path: $modelPath")
    }
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
