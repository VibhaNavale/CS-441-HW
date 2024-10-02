package MapReduce

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

class CosineSimilarityMapper extends Mapper[LongWritable, Text, Text, Text] {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  // A mutable list to store key-value pairs for printing later
  val emittedPairs = scala.collection.mutable.ListBuffer[(String, String)]()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    val line = value.toString.split("\\t")
    if (line.length == 2) {
      val tokenAndAdditional = line(0).trim.split(" ")
      val token = tokenAndAdditional.head
      val embedding = line(1).split(",").map(_.toDouble)

      // Emit the token and its embedding as a single string to the reducer
      val embeddingStr = embedding.mkString(",")
      context.write(new Text(token), new Text(embeddingStr))

      // Add emitted key-value pairs to the list
      emittedPairs += ((token, embeddingStr))
    } else {
      logger.warn(s"Skipping malformed input: $value")
    }
  }
}
