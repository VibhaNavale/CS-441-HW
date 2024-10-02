package MapReduce

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

object CosineSimilarityDriver {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    if (args.length < 2) {
      logger.error("Usage: CosineSimilarityDriver <input path> <output path>")
      System.exit(1)
    }

    val inputPathStr = config.getString("app.word2vecOutput")
    val outputPathStr = config.getString("app.cosineOutput")

    val configuration = new Configuration()
    val job = Job.getInstance(configuration, "Cosine Similarity Job")
    job.setJarByClass(this.getClass)

    // Set the mapper and reducer
    job.setMapperClass(classOf[CosineSimilarityMapper])
    job.setReducerClass(classOf[CosineSimilarityReducer])

    // Set output key and value classes
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    val conf: JobConf = new JobConf(this.getClass)
    conf.set("mapreduce.job.maps", "3")
    conf.set("mapreduce.job.reduces", "10")

    // Set input and output paths
    val inputPath = new Path(inputPathStr)
    val outputPath = new Path(outputPathStr)

    // Get the FileSystem instance
    val fs = FileSystem.get(conf)

    // Check if output path exists and delete if necessary
    if (fs.exists(outputPath)) {
      logger.warn(s"Deleting existing output path: ${outputPath}")
      fs.delete(outputPath, true)
    }

    FileInputFormat.addInputPath(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)

    // Run the job and return the success status
    if (!job.waitForCompletion(true)) {
      logger.error("Cosine Similarity job failed.")
      System.exit(1)
    }
  }
}
