package MapReduce

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.commons.math3.linear.Array2DRowRealMatrix

class CosineSimilarityReducer extends Reducer[Text, Text, Text, Text] {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  var tokenEmbeddings: Map[String, Array[Double]] = Map()

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val embedding = values.iterator().next().toString.split(",").map(_.toDouble)

    // Store the embedding vector
    tokenEmbeddings += (key.toString -> embedding)

    // Only perform cosine similarity calculations after gathering all embeddings
    if (tokenEmbeddings.size > 1) {
      // Create a list of token keys
      val keys = tokenEmbeddings.keys.toList

      // Convert token embeddings to a matrix
      val embeddingsMatrix = new Array2DRowRealMatrix(tokenEmbeddings.values.toArray)

      // Compute cosine similarities
      val results = for {
        i <- keys.indices
        j <- (i + 1) until keys.size
      } yield {
        val token1 = keys(i)
        val token2 = keys(j)
        val embedding1 = embeddingsMatrix.getRow(i)
        val embedding2 = embeddingsMatrix.getRow(j)

        val cosineSimilarity = calculateCosineSimilarity(embedding1, embedding2)
        val absCosineSimilarity = math.abs(cosineSimilarity)
        val classification = classifyCosineSimilarity(absCosineSimilarity)
        val result = f"$token1 $token2 $absCosineSimilarity%.4f $classification"

        // Return a pair of token and result
        (token1, result)
      }

      // Write results to context
      results.foreach { case (token1, result) => context.write(new Text(token1), new Text(result)) }
    }
  }

  def calculateCosineSimilarity(vecA: Array[Double], vecB: Array[Double]): Double = {
    val vectorA = new Array2DRowRealMatrix(vecA)
    val vectorB = new Array2DRowRealMatrix(vecB)

    val dotProduct = vectorA.transpose().multiply(vectorB).getEntry(0, 0)
    val magnitudeA = Math.sqrt(vectorA.getNorm)
    val magnitudeB = Math.sqrt(vectorB.getNorm)

    if (magnitudeA == 0.0 || magnitudeB == 0.0) 0.0 // Prevent division by zero
    else dotProduct / (magnitudeA * magnitudeB)
  }

  def classifyCosineSimilarity(similarity: Double): String = {
    if (similarity > 0.9) "Very Similar"
    else if (similarity > 0.7) "Similar"
    else "Dissimilar"
  }

  override def cleanup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    logger.info("Reducer complete.")
  }
}
