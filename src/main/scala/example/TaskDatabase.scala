package example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TaskDatabase {
  val DATABASE = "db"

  val USERS = "users"
  val PRODUCTS = "products"
  val CATEGORIES = "categories"
  val COMPLETED_ORDERS = "completed_orders"
  val NOT_COMPLETED_ORDERS = "not_completed_orders"

  def loadDataFrameFromDatabase(dbName: String, tableName: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .table(dbName + "." + tableName)
  }

  def createLocalDatabase(dbName: String)(implicit spark: SparkSession): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName;")
    spark.sql(s"USE $dbName;")
  }

  def saveDataFrameToTheLocalDatabase(tableName: String, df: DataFrame): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }

  def saveDataToTheLocalDatabase(dbName: String)(implicit spark: SparkSession): Unit = {
    createLocalDatabase(dbName)
    saveDataFrameToTheLocalDatabase(USERS, TaskData.createSampleUsersDF)
    saveDataFrameToTheLocalDatabase(PRODUCTS, TaskData.createSampleProductsDF)
    saveDataFrameToTheLocalDatabase(CATEGORIES, TaskData.createSampleCategoriesDF)
    saveDataFrameToTheLocalDatabase(COMPLETED_ORDERS, TaskData.createSampleCompletedOrdersDF)
    saveDataFrameToTheLocalDatabase(NOT_COMPLETED_ORDERS, TaskData.createSampleNotCompletedOrdersDF)
  }

  def loadDataFromTheLocalDatabase(implicit spark: SparkSession): Unit = {
    // LOAD TEST DATA
    loadDataFrameFromDatabase(TaskDatabase.DATABASE, TaskDatabase.USERS).show()
    loadDataFrameFromDatabase(TaskDatabase.DATABASE, TaskDatabase.PRODUCTS).show()
    loadDataFrameFromDatabase(TaskDatabase.DATABASE, TaskDatabase.CATEGORIES).show()
    loadDataFrameFromDatabase(TaskDatabase.DATABASE, TaskDatabase.COMPLETED_ORDERS).show()
    loadDataFrameFromDatabase(TaskDatabase.DATABASE, TaskDatabase.NOT_COMPLETED_ORDERS).show()
  }

  def main(args: Array[String]): Unit = {
    // CREATE SPARK SESSION
    implicit val spark: SparkSession = TaskInitialization.createLocalSparkSession
    // SAVE TEST DATA
    saveDataToTheLocalDatabase(DATABASE)
  }
}