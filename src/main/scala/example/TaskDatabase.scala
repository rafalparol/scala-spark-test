package example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/** Represents all the communication with Spark date warehouse tables and Parquet files.
 *
 */

object TaskDatabase {
  val DBWAREHOUSE = "spark-warehouse"
  val DATABASE = "db"

  val USERS = "users"
  val PRODUCTS = "products"
  val CATEGORIES = "categories"
  val COMPLETED_ORDERS = "completed_orders"
  val NOT_COMPLETED_ORDERS = "not_completed_orders"

  /** DF loaded from local data warehouse.
   *
   * @param tableName - table name
   * @param dbName - database name
   * @return DF loaded from local data warehouse.
   */
  def loadDataFrameFromDatabase(tableName: String, dbName: String = DATABASE)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .table(dbName + "." + tableName)
  }

  /** DF loaded from local data warehouse (Parquet files).
   *
   * @param tableName - table name
   * @param dbWarehousePath - local data warehouse path
   * @param dbName - database name
   * @return DF loaded from local data warehouse (Parquet files).
   */
  def loadDataFrameFromParquetFiles(tableName: String, dbWarehousePath: String = DBWAREHOUSE, dbName: String = DATABASE)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .parquet(dbWarehousePath + "/" + dbName + "." + dbName + "/" + tableName)
  }

  /** DF with users loaded from local data warehouse (Parquet files).
   *
   * @return DF with users loaded from local data warehouse (Parquet files).
   */
  def loadSampleUsersDF(implicit spark: SparkSession): DataFrame = {
    loadDataFrameFromParquetFiles(USERS)
  }

  /** DF with completed orders loaded from local data warehouse (Parquet files).
   *
   * @return DF with completed orders loaded from local data warehouse (Parquet files).
   */
  def loadSampleCompletedOrdersDF(implicit spark: SparkSession): DataFrame = {
    loadDataFrameFromParquetFiles(COMPLETED_ORDERS)
  }

  /** DF with not completed orders loaded from local data warehouse (Parquet files).
   *
   * @return DF with not completed orders loaded from local data warehouse (Parquet files).
   */
  def loadSampleNotCompletedOrdersDF(implicit spark: SparkSession): DataFrame = {
    loadDataFrameFromParquetFiles(NOT_COMPLETED_ORDERS)
  }

  /** DF with products loaded from local data warehouse (Parquet files).
   *
   * @return DF with products loaded from local data warehouse (Parquet files).
   */
  def loadSampleProductsDF(implicit spark: SparkSession): DataFrame = {
    loadDataFrameFromParquetFiles(PRODUCTS)
  }

  /** DF with categories loaded from local data warehouse (Parquet files).
   *
   * @return DF with categories loaded from local data warehouse (Parquet files).
   */
  def loadSampleCategoriesDF(implicit spark: SparkSession): DataFrame = {
    loadDataFrameFromParquetFiles(CATEGORIES)
  }

  /** All DFs loaded from local data warehouse and printed.
   *
   */
  def loadDataFromTheLocalDatabase(implicit spark: SparkSession): Unit = {
    // LOAD TEST DATA
    loadDataFrameFromDatabase(TaskDatabase.USERS).show()
    loadDataFrameFromDatabase(TaskDatabase.PRODUCTS).show()
    loadDataFrameFromDatabase(TaskDatabase.CATEGORIES).show()
    loadDataFrameFromDatabase(TaskDatabase.COMPLETED_ORDERS).show()
    loadDataFrameFromDatabase(TaskDatabase.NOT_COMPLETED_ORDERS).show()
  }

  /** Creation of database if not exists in the local data warehouse.
   * @param dbName - database name
   */
  def createLocalDatabase(dbName: String)(implicit spark: SparkSession): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName;")
    spark.sql(s"USE $dbName;")
  }

  /** Saving a single DF to the local data warehouse.
   * @param tableName - table name
   * @param df - DF to be saved
   */
  def saveDataFrameToTheLocalDatabase(tableName: String, df: DataFrame): Unit = {
    df
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }

  /** Creation of database if not exists in the local data warehouse. Saving all DFs to the local data warehouse.
   * @param dbName - database name
   */
  def saveDataToTheLocalDatabase(dbName: String)(implicit spark: SparkSession): Unit = {
    createLocalDatabase(dbName)
    saveDataFrameToTheLocalDatabase(USERS, TaskData.createSampleUsersDF)
    saveDataFrameToTheLocalDatabase(PRODUCTS, TaskData.createSampleProductsDF)
    saveDataFrameToTheLocalDatabase(CATEGORIES, TaskData.createSampleCategoriesDF)
    saveDataFrameToTheLocalDatabase(COMPLETED_ORDERS, TaskData.createSampleCompletedOrdersDF)
    saveDataFrameToTheLocalDatabase(NOT_COMPLETED_ORDERS, TaskData.createSampleNotCompletedOrdersDF)
  }

  /** Creation of the Test Spark Session object. Creation of database if not exists in the local data warehouse. Saving all DFs to the local data warehouse.
   *  Second main method to fill the local data warehouse with data before running unit tests or the other first main function.
   */
  def main(args: Array[String]): Unit = {
    // CREATE SPARK SESSION
    implicit val spark: SparkSession = TaskInitialization.createLocalSparkSession
    // SAVE TEST DATA
    saveDataToTheLocalDatabase(DATABASE)
  }
}