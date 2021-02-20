import java.io.{File, FileWriter}

import init.Init.initSparkSession
import helper.ArgsHelper.argsHelper
import model.CountMetric
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.OutputMode

object Main extends Serializable {
  def main(args: Array[String]) {

    // get parameters from cli commands
    val parameters = argsHelper(args)

    // Init spark session
    val spark = initSparkSession("VeevaMergeFiles", parameters.getOrElse("Master", "local"))
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    // Read the given input directory in streaming
    val data = spark
      .readStream
      .option("maxFilesPerTrigger", 1)
      .text(s"input/*")


    // Merges all files in the input directory (we could add also a new files into directory while this application is running)
    // And writes them into output directory by partitioning with first letter of the element
    // In the output directory we will have as many subdirectories as the unique first letters in all elements in all files
    // all the elements with the same first letter will be grouped in the same subdirectory
    data
      .withColumn("partitionCol", col("value").substr(0, 1))
      .repartition(col("partitionCol"))
      .writeStream
      .partitionBy("partitionCol")
      .outputMode(OutputMode.Append())
      .format("text")
      .option("path", s"output/merged-files/")
      .option("checkpointLocation", s"checkpoint/merged-files/")
      .start()


    val writerForText: ForeachWriter[(String, CountMetric)] = createWriterForCountMetric()

    // Merges all files in the input directory (we could add also a new files into directory while this application is running)
    // And writes them into output directory with the total number of occurrence of that element in all files.
    // This count value is updated everytime we add new file to the directory.
    // In the output directory we will one file containing all the summary of key and count pair
    data
      .withColumn("count", lit(1L))
      .as[CountMetric]
      .groupByKey(_.value)
      .reduceGroups((one, two) => CountMetric(one.value, one.count + two.count))
      .coalesce(1)
      .writeStream
      .outputMode(OutputMode.Complete())
      .foreach(writerForText)
      .option("path", s"output/count-metric/")
      .option("checkpointLocation", s"checkpoint/count-metric/")
      .start()

    spark.streams.awaitAnyTermination()

  }

  /***
   * Custom writer to support complete output mode for the text writer of count metric
   * @return
   */
  def createWriterForCountMetric(): ForeachWriter[(String, CountMetric)] = {
    new ForeachWriter[(String, CountMetric)] {
      var fileWriter: FileWriter = _

      override def process(value: (String, CountMetric)): Unit = {
        fileWriter.append("key: " + value._1 + " count: " + value._2.count.toString + "\n")
      }

      override def close(errorOrNull: Throwable): Unit = {
        fileWriter.close()
      }

      override def open(partitionId: Long, version: Long): Boolean = {
        FileUtils.forceMkdir(new File(s"output/count-metric/${partitionId}"))
        fileWriter = new FileWriter(new File(s"output/count-metric/${partitionId}/temp.txt"))
        true

      }
    }
  }

}
