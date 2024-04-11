package example

import org.apache.spark.sql.{DataFrame, SparkSession}

object TaskData {
  // TEST DATA METHODS

  def createSampleUsers: Seq[(String, String, String, String, String, String, String)] = List(
    ("user-01", "Anne", "Anderson", "USA", "Boston", "02138", "19 Ware St, Cambridge, MA 02138, USA"),
    ("user-02", "Tommy", "Harada", "Japan", "Ebina", "243-0402", "555-1 Kashiwagaya, Ebina, Kanagawa 243-0402, Japan"),
    ("user-03", "Stephane", "Moreau", "France", "Paris", "75003", "14 R. des Minimes, 75003 Paris, France")
  )

  def createSampleCompletedOrders: Seq[(String, String, String, String, String, Long, Double, Double, String, String, String)] = Seq(
    ("order-01", "payment-01", "user-01", "laptop-01", "USA", 1L, 100.0, 20.0, "2023-02-01 00:00:00", "2023-02-09 00:00:00", "COMPLETED"),
    ("order-02", "payment-02", "user-02", "laptop-02", "Japan", 2L, 400.0, 80.0, "2023-03-01 00:00:00", "2023-03-09 00:00:00", "COMPLETED"),
    ("order-03", "payment-03", "user-03", "laptop-03", "France", 1L, 300.0, 30.0, "2023-04-01 00:00:00", "2023-04-09 00:00:00", "COMPLETED"),
    ("order-04", "payment-04", "user-01", "laptop-05", "USA", 2L, 500.0, 20.0, "2023-05-01 00:00:00", "2023-05-09 00:00:00", "COMPLETED")
  )

  def createSampleNotCompletedOrders: Seq[(String, String, String, String, String, Long, Double, Double, String, String, String)] = Seq(
    ("order-05", "payment-05", "user-02", "laptop-06", "Japan", 1L, 350.0, 40.0, "2023-07-01 00:00:00", "", "NOT COMPLETED"),
    ("order-06", "payment-06", "user-03", "laptop-07", "France", 2L, 200.0, 160.0, "2023-07-01 00:00:00", "", "NOT COMPLETED")
  )

  def createSampleProducts: Seq[(String, String, String, String, Double, Double, String)] = Seq(
    ("laptop-01", "office-laptops-01", "HP", "USA", 100.0, 20.0, "2023-01-01"),
    ("laptop-02", "regular-laptops-01", "Dell", "Japan", 200.0, 40.0, "2023-02-01"),
    ("laptop-03", "super-laptops-01", "Acer", "France", 300.0, 30.0, "2023-03-01"),
    ("laptop-04", "super-laptops-02", "HP", "Germany", 150.0, 50.0, "2023-02-01"),
    ("laptop-05", "office-laptops-01", "Dell", "USA", 250.0, 10.0, "2023-04-01"),
    ("laptop-06", "premium-laptops-01", "Asus", "Japan", 350.0, 40.0, "2023-06-01"),
    ("laptop-07", "ultra-laptops-01", "Apple", "France", 100.0, 80.0, "2023-02-01"),
    ("laptop-08", "ultra-laptops-02", "Acer", "Germany", 200.0, 60.0, "2023-03-01"),
    ("laptop-09", "gaming-laptops-01", "Acer", "USA", 200.0, 40.0, "2023-02-01"),
    ("laptop-10", "gaming-laptops-01", "Asus", "USA", 100.0, 40.0, "2023-01-01")
  )

  def createSampleCategories: Seq[(String, String, String, String)] = Seq(
    ("office-laptops-01", "office-laptops", "ol-01", "USA"),
    ("gaming-laptops-01", "gaming-laptops", "gl-01", "USA"),
    ("regular-laptops-01", "regular-laptops", "rl-01", "Japan"),
    ("premium-laptops-01", "premium-laptops", "pl-01", "Japan"),
    ("super-laptops-01", "super-laptops", "sl-01", "France"),
    ("ultra-laptops-01", "ultra-laptops", "ul-01", "France"),
    ("super-laptops-02", "super-laptops", "sl-02", "Germany"),
    ("ultra-laptops-02", "ultra-laptops", "ul-02", "Germany")
  )

  def createSampleCompletedPayments: Seq[(String, String, Double, String, String, String)] = Seq(
    ("payment-01", "USA", 100.0, "2023-02-15 00:00:00", "2023-02-08 00:00:00", "COMPLETED"),
    ("payment-02", "Japan", 400.0, "2023-03-15 00:00:00", "2023-03-08 00:00:00", "COMPLETED"),
    ("payment-03", "France", 300.0, "2023-04-15 00:00:00", "2023-04-08 00:00:00", "COMPLETED"),
    ("payment-04", "USA", 500.0, "2023-05-15 00:00:00", "2023-05-08 00:00:00", "COMPLETED")
  )

  def createSampleNotCompletedPayments: Seq[(String, String, Double, String, String, String)] = Seq(
    ("payment-05", "Japan", 350.0, "2023-07-15 00:00:00", "", "NOT COMPLETED"),
    ("payment-06", "France", 200.0, "2023-07-15 00:00:00", "", "NOT COMPLETED")
  )

  def createSampleUsersDF(spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleUsers)
      .withColumnRenamed("_1", "UserId")
      .withColumnRenamed("_2", "FirstName")
      .withColumnRenamed("_3", "LastName")
      .withColumnRenamed("_4", "Country")
      .withColumnRenamed("_5", "City")
      .withColumnRenamed("_6", "PostalCode")
      .withColumnRenamed("_7", "Address")
  }

  def createSampleCompletedOrdersDF(spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleCompletedOrders)
      .withColumnRenamed("_1", "OrderId")
      .withColumnRenamed("_2", "PaymentId")
      .withColumnRenamed("_3", "UserId")
      .withColumnRenamed("_4", "ProductId")
      .withColumnRenamed("_5", "Country")
      .withColumnRenamed("_6", "Count")
      .withColumnRenamed("_7", "TotalValue")
      .withColumnRenamed("_8", "TotalWeight")
      .withColumnRenamed("_9", "OrderGenerationDate")
      .withColumnRenamed("_10", "OrderCompletionDate")
      .withColumnRenamed("_11", "Status")
  }

  def createSampleNotCompletedOrdersDF(spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleNotCompletedOrders)
      .withColumnRenamed("_1", "OrderId")
      .withColumnRenamed("_2", "PaymentId")
      .withColumnRenamed("_3", "UserId")
      .withColumnRenamed("_4", "ProductId")
      .withColumnRenamed("_5", "Country")
      .withColumnRenamed("_6", "Count")
      .withColumnRenamed("_7", "TotalValue")
      .withColumnRenamed("_8", "TotalWeight")
      .withColumnRenamed("_9", "OrderGenerationDate")
      .withColumnRenamed("_10", "OrderCompletionDate")
      .withColumnRenamed("_11", "Status")
  }

  def createSampleProductsDF(spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleProducts)
      .withColumnRenamed("_1", "ProductId")
      .withColumnRenamed("_2", "CategoryId")
      .withColumnRenamed("_3", "Name")
      .withColumnRenamed("_4", "Country")
      .withColumnRenamed("_5", "Price")
      .withColumnRenamed("_6", "Weight")
      .withColumnRenamed("_7", "MarketEntranceDate")
  }

  def createSampleCategoriesDF(spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleCategories)
      .withColumnRenamed("_1", "CategoryId")
      .withColumnRenamed("_2", "Name")
      .withColumnRenamed("_3", "Code")
      .withColumnRenamed("_4", "Country")
  }

  def createSampleCompletedPaymentsDF(spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleCompletedPayments)
      .withColumnRenamed("_1", "PaymentId")
      .withColumnRenamed("_2", "Country")
      .withColumnRenamed("_3", "TotalValue")
      .withColumnRenamed("_4", "PaymentDeadline")
      .withColumnRenamed("_5", "PaymentCompletionDate")
      .withColumnRenamed("_6", "PaymentStatus")
  }

  def createSampleNotCompletedPaymentsDF(spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleNotCompletedPayments)
      .withColumnRenamed("_1", "PaymentId")
      .withColumnRenamed("_2", "Country")
      .withColumnRenamed("_3", "TotalValue")
      .withColumnRenamed("_4", "PaymentDeadline")
      .withColumnRenamed("_5", "PaymentCompletionDate")
      .withColumnRenamed("_6", "PaymentStatus")
  }
}
