package example

import org.apache.spark.sql.SparkSession

object TaskInitialization {
  // CREATE SPARK SESSION

  def createSparkSession: SparkSession = {
    SparkSession.builder
      .appName("Spark Scala Task")
   // .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
   // .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .getOrCreate()
  }

  def createLocalSparkSession: SparkSession = {
    SparkSession.builder
      .appName("Spark Scala Task")
      .master("local[*]")
   // .config("spark.sql.warehouse.dir", "spark-warehouse")
   // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }
}
