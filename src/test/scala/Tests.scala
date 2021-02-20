import java.io.{File, FileWriter}

import com.merge.model.CountMetric
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class Tests extends AnyFunSuite with BeforeAndAfterEach {

  // It's proper to put the code in a function and call the function of course
  // It's just for demonstration.

  test("Unit test merge count result") {

    val spark = SparkSession.builder().appName("udf testings")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val data = spark
      .readStream
      .option("maxFilesPerTrigger", 1)
      .text(s"src/test/input/*")


    // asserts are done in the foreach writer object
    val writerForText: ForeachWriter[(String, CountMetric)] = createWriterForCountMetric()

    val elements = data
      .withColumn("count", lit(1L))
      .as[CountMetric]
      .groupByKey(_.value)
      .reduceGroups((one, two) => CountMetric(one.value, one.count + two.count))
      .coalesce(1)
      .writeStream
      .outputMode(OutputMode.Complete())
      .foreach(writerForText)
      .option("path", s"src/test/output/count-metric/")
      .option("checkpointLocation", s"src/test/checkpoint/count-metric/")
      .start()

    spark.stop()

  }


  def createWriterForCountMetric(): ForeachWriter[(String, CountMetric)] = {
    new ForeachWriter[(String, CountMetric)] {
      var fileWriter: FileWriter = _

      override def process(value: (String, CountMetric)): Unit = {
        val firstExpectedMetric: CountMetric = CountMetric("Tristan", 1)
        val secondExpectedMetric: CountMetric = CountMetric("Zander", 1)

        if ("Tristan".equals(value._1)) {
          assert(firstExpectedMetric.equals(value), "Should be equals")
        }


        if ("Zander".equals(value._1)) {
          assert(secondExpectedMetric.equals(value), "Should be equals")
        }

        fileWriter.append("key: " + value._1 + " count: " + value._2.count.toString + "\n")
      }

      override def close(errorOrNull: Throwable): Unit = {
        fileWriter.close()
      }

      override def open(partitionId: Long, version: Long): Boolean = {
        FileUtils.forceMkdir(new File(s"src/test/output/count-metric/${partitionId}"))
        fileWriter = new FileWriter(new File(s"src/test/output/count-metric/${partitionId}/temp.txt"))
        true

      }
    }
  }
}
