package MapReduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}

object Word2VecDriver {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val inputPathStr = config.getString("app.tokenizerOutput")
    val outputPathStr = config.getString("app.word2vecOutput")

    val conf = new Configuration()
    val job = Job.getInstance(conf, "Word2Vec Job")
    job.setJarByClass(this.getClass)

    // Set mapper and reducer
    job.setMapperClass(classOf[Word2VecMapper])
    job.setReducerClass(classOf[Word2VecReducer])

    // Set output key and value classes
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    // Set input and output paths
    val inputPath = new Path(inputPathStr) // Tokenized output path from config
    val outputPath = new Path(outputPathStr) // Output path for Word2Vec

    // Get the FileSystem instance
    val fs = FileSystem.get(conf)

    // Check if output path exists and delete if necessary
    if (fs.exists(outputPath)) {
      logger.info(s"Deleting existing output path: ${outputPath}")
      fs.delete(outputPath, true)
    }

    FileInputFormat.addInputPath(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)

    // Run the job and return the success status
    val jobSuccess = job.waitForCompletion(true)
    if (!jobSuccess) {
      logger.error("Word2Vec job failed.") // Log error if the job fails
      System.exit(1)
    }
  }
}
