package example

import org.apache.spark.sql.SparkSession

object TaskInitialization {
  // CREATE SPARK SESSION

  def createSparkSession: SparkSession = SparkSession.builder
    .appName("Spark Scala Task")
    .getOrCreate()

  def createLocalSparkSession: SparkSession = {
    val ss = SparkSession.builder
      .appName("Spark Scala Task")
      .master("local[*]")
      .getOrCreate()
    ss.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    ss.conf.set("spark.sql.session.timeZone", "UTC")
    ss
  }
}
