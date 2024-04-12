package example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TaskDatabase {
  val DBWAREHOUSE = "spark-warehouse"
  val DATABASE = "db"

  val USERS = "users"
  val PRODUCTS = "products"
  val CATEGORIES = "categories"
  val COMPLETED_ORDERS = "completed_orders"
  val NOT_COMPLETED_ORDERS = "not_completed_orders"

  def loadDataFrameFromDatabase(tableName: String, dbName: String = DATABASE)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .table(dbName + "." + tableName)
  }

  def loadDataFrameFromParquetFiles(tableName: String, dbWarehousePath: String = DBWAREHOUSE, dbName: String = DATABASE)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .parquet(dbWarehousePath + "/" + dbName + "." + dbName + "/" + tableName)
  }

  def loadSampleUsersDF(implicit spark: SparkSession): DataFrame = {
    loadDataFrameFromParquetFiles(USERS)
  }

  def loadSampleCompletedOrdersDF(implicit spark: SparkSession): DataFrame = {
    loadDataFrameFromParquetFiles(COMPLETED_ORDERS)
  }

  def loadSampleNotCompletedOrdersDF(implicit spark: SparkSession): DataFrame = {
    loadDataFrameFromParquetFiles(NOT_COMPLETED_ORDERS)
  }

  def loadSampleProductsDF(implicit spark: SparkSession): DataFrame = {
    loadDataFrameFromParquetFiles(PRODUCTS)
  }

  def loadSampleCategoriesDF(implicit spark: SparkSession): DataFrame = {
    loadDataFrameFromParquetFiles(CATEGORIES)
  }

  def loadDataFromTheLocalDatabase(implicit spark: SparkSession): Unit = {
    // LOAD TEST DATA
    loadDataFrameFromDatabase(TaskDatabase.USERS).show()
    loadDataFrameFromDatabase(TaskDatabase.PRODUCTS).show()
    loadDataFrameFromDatabase(TaskDatabase.CATEGORIES).show()
    loadDataFrameFromDatabase(TaskDatabase.COMPLETED_ORDERS).show()
    loadDataFrameFromDatabase(TaskDatabase.NOT_COMPLETED_ORDERS).show()
  }

  def createLocalDatabase(dbName: String)(implicit spark: SparkSession): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName;")
    spark.sql(s"USE $dbName;")
  }

  def saveDataFrameToTheLocalDatabase(tableName: String, df: DataFrame): Unit = {
    df
      .write
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

  def main(args: Array[String]): Unit = {
    // CREATE SPARK SESSION
    implicit val spark: SparkSession = TaskInitialization.createLocalSparkSession
    // SAVE TEST DATA
    saveDataToTheLocalDatabase(DATABASE)
  }
}