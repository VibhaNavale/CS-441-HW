package MapReduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory
import java.io.{BufferedWriter, FileWriter}
import java.nio.file
import java.nio.file.Files
import scala.util.Try

class TokenizedDriverSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val logger = LoggerFactory.getLogger(getClass)
  private var inputDir: file.Path = _
  private var outputDir: file.Path = _
  private val conf = new Configuration()
  private val fs = FileSystem.get(conf)

  override def beforeAll(): Unit = {
    // Create temporary directories
    inputDir = Files.createTempDirectory("input")
    outputDir = Files.createTempDirectory("output")

    // Set up input data
    val inputFile = new Path(inputDir.toString + "/input.txt")
    val outputStream = fs.create(inputFile)
    val writer = new BufferedWriter(new FileWriter(outputStream.toString))
    writer.write("review,sentiment\n")
    writer.write("This is a great movie!,positive\n")
    writer.write("It was a terrible experience.,negative\n")
    writer.write("I would recommend it to everyone.,positive\n")
    writer.close()
  }

  override def afterAll(): Unit = {
    // Clean up temporary directories
    if (fs.exists(new Path(inputDir.toString))) {
      fs.delete(new Path(inputDir.toString), true)
    }
    if (fs.exists(new Path(outputDir.toString))) {
      fs.delete(new Path(outputDir.toString), true)
    }
  }

  test("TokenizedDriver should run successfully with valid input and output paths") {
    val args = Array(inputDir.toString + "/input.txt", outputDir.toString)
    Try {
      TokenizedDriver.main(args)
    } match {
      case scala.util.Success(_) => logger.info("Test passed: Job completed successfully.")
      case scala.util.Failure(exception) => fail(s"Test failed: ${exception.getMessage}")
    }

    assert(fs.exists(new Path(outputDir.toString)), "Output path should exist after job completion.")
  }

  test("TokenizedDriver should fail with non-existing input path") {
    val invalidInputPath = "invalid_input"
    val args = Array(invalidInputPath, outputDir.toString)

    // Redirecting System.exit to avoid test termination
    val exitCode = new java.io.ByteArrayOutputStream()
    System.setOut(new java.io.PrintStream(exitCode))

    Try {
      TokenizedDriver.main(args)
    } match {
      case scala.util.Success(_) => fail("Job should have failed due to invalid input path.")
      case _ => // Expected failure
    }

    val output = exitCode.toString
    assert(output.contains("Input path does not exist:"), "Should log an error for non-existing input path.")
  }

  test("TokenizedDriver should overwrite existing output directory") {
    // Create an output directory to test the overwrite functionality
    val existingOutputDir = new Path(outputDir.toString)
    if (!fs.exists(existingOutputDir)) {
      fs.mkdirs(existingOutputDir)
    }

    val args = Array(inputDir.toString + "/input.txt", outputDir.toString)
    Try {
      TokenizedDriver.main(args)
    } match {
      case scala.util.Success(_) => logger.info("Test passed: Job completed successfully.")
      case scala.util.Failure(exception) => fail(s"Test failed: ${exception.getMessage}")
    }

    // Ensure the output directory has been recreated
    assert(fs.exists(new Path(outputDir.toString)), "Output path should exist after job completion.")
  }

  test("TokenizedDriver should fail if output path is missing in arguments") {
    val args = Array(inputDir.toString + "/input.txt")

    // Redirecting System.exit to avoid test termination
    val exitCode = new java.io.ByteArrayOutputStream()
    System.setOut(new java.io.PrintStream(exitCode))

    Try {
      TokenizedDriver.main(args)
    } match {
      case scala.util.Success(_) => fail("Job should have failed due to missing output path argument.")
      case _ => // Expected failure
    }

    val output = exitCode.toString
    assert(output.contains("Output path argument is missing."), "Should log an error for missing output path.")
  }

  test("TokenizedDriver should log and exit with a warning for invalid output path") {
    // Test with an invalid output path
    val invalidOutputPath = "invalid_output_path"
    val args = Array(inputDir.toString + "/input.txt", invalidOutputPath)

    // Redirecting System.exit to avoid test termination
    val exitCode = new java.io.ByteArrayOutputStream()
    System.setOut(new java.io.PrintStream(exitCode))

    Try {
      TokenizedDriver.main(args)
    } match {
      case scala.util.Success(_) => fail("Job should have failed due to invalid output path.")
      case _ => // Expected failure
    }

    val output = exitCode.toString
    assert(output.contains("Failed to set output path:"), "Should log an error for invalid output path.")
  }
}
