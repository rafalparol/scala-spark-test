package example

import org.apache.spark.sql.SparkSession

/** Represents creation of Spark contexts.
 *
 */

object TaskInitialization {
  // CREATE SPARK SESSION

  /** Spark session for Production code.
   *
   * @return Spark session object.
   */
  def createSparkSession: SparkSession = {
    SparkSession.builder
      .appName("Spark Scala Task")
   // .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
   // .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .getOrCreate()
  }

  /** Spark session for Test code.
   *
   * @return Spark session object.
   */
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
