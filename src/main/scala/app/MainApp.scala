package app

import MapReduce.{TokenizedDriver, Word2VecDriver, CosineSimilarityDriver}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException

object MainApp {
  private val config = ConfigFactory.load() // Load configuration
  private val logger = LoggerFactory.getLogger(getClass) // Initialize the logger

  def main(args: Array[String]): Unit = {

    // Extract input and output paths from arguments
    val tokenizerInput = config.getString("app.inputPath")
    val tokenizerOutput = config.getString("app.tokenizerOutput")

    // Define output paths for Word2Vec and Cosine Similarity using config
    val word2vecOutput = config.getString("app.word2vecOutput") // Get Word2Vec output path from config
    val cosineOutput = config.getString("app.cosineOutput")     // Get Cosine Similarity output path from config

    // Run Tokenizer
    logger.info("Starting tokenization process...")
    runTokenizer(tokenizerInput, tokenizerOutput)

    // Check if the tokenization output directory exists and has any part files
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val tokenizedDir = new Path(tokenizerOutput)

    try {
      val status: FileStatus = fs.getFileStatus(tokenizedDir)
      if (!status.isDirectory || fs.listStatus(tokenizedDir).isEmpty) {
        logger.error(s"Tokenization output directory not found or empty: ${tokenizedDir.toString}")
        System.exit(1) // Exit if tokenization failed
      }
    } catch {
      case _: FileNotFoundException =>
        logger.error(s"Tokenization output directory not found: ${tokenizedDir.toString}")
        System.exit(1) // Exit if directory does not exist
      case e: Exception =>
        logger.error(s"An error occurred while checking tokenization output directory: ${e.getMessage}", e)
        System.exit(1) // Exit on other exceptions
    }

    // Run Word2Vec
    logger.info("Tokenization completed. Starting Word2Vec process...")
    runWord2Vec(tokenizerOutput, word2vecOutput)

    // Run Cosine Similarity
    logger.info("Word2Vec process completed. Starting Cosine Similarity process...")
    runCosineSimilarity(word2vecOutput, cosineOutput)

    logger.info("All processes completed successfully.")
  }

  private def runTokenizer(input: String, output: String): Unit = {
    try {
      logger.info(s"Running tokenizer with input: $input and output: $output")
      TokenizedDriver.main(Array(input, output))
      logger.info("********* Tokenization process completed *********")
    } catch {
      case e: Exception =>
        logger.error(s"An error occurred during tokenization: ${e.getMessage}", e)
    }
  }

  private def runWord2Vec(input: String, output: String): Unit = {
    try {
      Word2VecDriver.main(Array(input, output))
      logger.info("********* Word2Vec process completed *********")
    } catch {
      case e: Exception =>
        logger.error(s"An error occurred during Word2Vec process: ${e.getMessage}", e)
    }
  }

  private def runCosineSimilarity(input: String, output: String): Unit = {
    try {
      CosineSimilarityDriver.main(Array(input, output))
      logger.info("********* Cosine similarity process completed *********")
    } catch {
      case e: Exception =>
        logger.error(s"An error occurred during Cosine Similarity process: ${e.getMessage}", e)
    }
  }
}
