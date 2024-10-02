package MapReduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.mapred.JobConf

object TokenizedDriver {
  private val logger = LoggerFactory.getLogger(getClass) // Initialize logger
  private val config = ConfigFactory.load() // Load configuration

  def main(args: Array[String]): Unit = {
    // Set up the configuration and job
    val configuration = new Configuration()
    val job = Job.getInstance(configuration, "Tokenization Job")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizedMapper])
    job.setReducerClass(classOf[TokenizedReducer])

    // Set output key and value classes
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    // Check if input path exists
    val inputPath = new Path(args(0))
    val fs = inputPath.getFileSystem(configuration)
    if (!fs.exists(inputPath)) {
      logger.error(s"Input path does not exist: ${inputPath}")
      System.exit(1)
    }

    val numMappers = config.getInt("app.numMappers") // Number of mappers from config
    val numReducers = config.getInt("app.numReducers")

    val conf: JobConf = new JobConf(this.getClass)
    conf.setInt("mapreduce.job.maps", numMappers)
    conf.setInt("mapreduce.job.reduces", numReducers)
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "67108864") // 64MB splits

    // Set input and output paths
    FileInputFormat.addInputPath(job, inputPath)
    val outputPath = new Path(args(1))

    // Check if output path exists and delete if necessary
    if (fs.exists(outputPath)) {
      logger.warn(s"Deleting existing output path: ${outputPath}")
      fs.delete(outputPath, true)
    }

    FileOutputFormat.setOutputPath(job, outputPath)

    // Run the job
    val jobCompletionStatus = job.waitForCompletion(true)
    if (!jobCompletionStatus) {
      logger.error("Tokenization job failed.")
      System.exit(1)
    }

    logger.info("Tokenization job completed successfully.")
  }
}
