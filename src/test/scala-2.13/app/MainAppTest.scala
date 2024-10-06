package app

import MapReduce.{CosineSimilarityDriver, TokenizedDriver, Word2VecDriver}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import com.typesafe.config.Config
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

import java.io.FileNotFoundException

class MainAppTest extends AnyFunSuite with MockitoSugar with BeforeAndAfter {

  private val logger = mock[Logger]
  private val config = mock[Config]
  private val fs = mock[FileSystem]

  before {
    // Set up common config mocks
    when(config.getString("app.inputPath")).thenReturn("s3a://cs441-assignment1/input/IMDB.csv")
    when(config.getString("app.tokenizerOutput")).thenReturn("s3a://cs441-assignment1/output/tokenizer_output")
    when(config.getString("app.word2vecOutput")).thenReturn("s3a://cs441-assignment1/output/word2vec_output")
    when(config.getString("app.cosineOutput")).thenReturn("s3a://cs441-assignment1/output/cosine_output")
  }

  // Test 1: runTokenizer should call TokenizedDriver's main method
  test("runTokenizer should call TokenizedDriver's main method") {
    val input = "s3a://input/path"
    val output = "s3a://output/path"

    MainApp.runTokenizer(input, output)

    verify(logger).info(s"Running tokenizer with input: $input and output: $output")
    verify(logger).info("********* Tokenization process completed *********")
  }

  // Test 2: runWord2Vec should call Word2VecDriver's main method
  test("runWord2Vec should call Word2VecDriver's main method") {
    val input = "s3a://input/path"
    val output = "s3a://output/path"

    MainApp.runWord2Vec(input, output)

    verify(logger).info("********* Word2Vec process completed *********")
  }

  // Test 3: runCosineSimilarity should call CosineSimilarityDriver's main method
  test("runCosineSimilarity should call CosineSimilarityDriver's main method") {
    val input = "s3a://input/path"
    val output = "s3a://output/path"

    MainApp.runCosineSimilarity(input, output)

    verify(logger).info("********* Cosine similarity process completed *********")
  }

  // Test 4: Should log error and exit when tokenization output directory does not exist
  test("Should log error and exit when tokenization output directory does not exist") {
    when(fs.getFileStatus(any[Path])).thenThrow(new FileNotFoundException())

    intercept[Exception] {
      MainApp.main(Array.empty)
    }

    verify(logger).error(s"Tokenization output directory not found: s3a://cs441-assignment1/output/tokenizer_output")
  }

  // Test 5: Should log error and exit when tokenization directory is empty
  test("Should log error and exit when tokenization directory is empty") {
    when(fs.listStatus(any[Path])).thenReturn(Array.empty)

    intercept[Exception] {
      MainApp.main(Array.empty)
    }

    verify(logger).error(s"Tokenization output directory not found or empty: s3a://cs441-assignment1/output/tokenizer_output")
  }
}
