package init

import org.apache.spark.sql.SparkSession

object Init {
  def initSparkSession(appName: String, master: String):SparkSession = {
    SparkSession
    .builder
    .appName(appName)
    .master(master)
    .getOrCreate()

  }
}
