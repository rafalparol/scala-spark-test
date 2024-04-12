package example

import org.apache.spark.sql.{DataFrame, SparkSession}

object TaskData {
  val COMPLETED_ORDERS_COUNT = 50000
  val NOT_COMPLETED_ORDERS_COUNT = 50000
  val USERS_COUNT = 10000
  val INSTANCES_COUNT = 5
  val COUNTRIES_COUNT = 10
  val MANUFACTURERS_COUNT = 20
  val PRODUCTS_COUNT = 1000
  val CATEGORIES_COUNT = 100

  val PRICE_INTERVAL = 100
  val MAX_PRICE = 500

  val WEIGHT_INTERVAL = 20
  val MAX_WEIGHT = 100

  val MAX_DAYS = 28

  // DYNAMICALLY GENERATED

  // TODO: The logic of generating data dynamically could be improved to maintain the consistency between products, countries, users, categories etc.
  //       Not all products exist in all countries, users come from different countries but they do not change their place of residence, etc.
  //       It would probably be better to rely on generating all IDs using predefined existing sets of products, categories, users and relationships between them.
  //       However, it should not matter too much for performance testing.
  //       The code may contain some bugs.

  def createDynamicallyGeneratedSampleUser(id: Int): (String, String, String, String, String, String, String) =
    (s"user-$id", s"FirstName-$id", s"LastName-$id", s"Country-${id % COUNTRIES_COUNT + 1}", s"City-$id", s"PostalCode-$id", s"Address-$id")

  def createDynamicallyGeneratedSampleUsers(count: Int): Seq[(String, String, String, String, String, String, String)] =
    (1 to count).map(i => createDynamicallyGeneratedSampleUser(i)).toList

  def createDynamicallyGeneratedSampleProduct(id: Int): (String, String, String, String, Double, Double, String) =
    (s"product-$id", s"category-${id % CATEGORIES_COUNT + 1}", s"Manufacturer-${id % MANUFACTURERS_COUNT + 1}", s"Country-${id % COUNTRIES_COUNT + 1}", id * PRICE_INTERVAL % MAX_PRICE + PRICE_INTERVAL, id * WEIGHT_INTERVAL % MAX_WEIGHT + WEIGHT_INTERVAL, f"2023-01-${id % MAX_DAYS + 1}%02d")

  def createDynamicallyGeneratedSampleProducts(count: Int): Seq[(String, String, String, String, Double, Double, String)] =
    (1 to count).map(i => createDynamicallyGeneratedSampleProduct(i)).toList

  def createDynamicallyGeneratedSampleCategory(id: Int): (String, String, String, String) =
    (s"category-$id", s"Name-$id", s"Code-$id", s"Country-${id % COUNTRIES_COUNT + 1}")

  def createDynamicallyGeneratedSampleCategories(count: Int): Seq[(String, String, String, String)] =
    (1 to count).map(i => createDynamicallyGeneratedSampleCategory(i)).toList

  def createDynamicallyGeneratedSampleCompletedOrder(id: Int, price: Double, weight: Double): (String, String, String, String, String, Long, Double, Double, String, String, String) =
    (s"order-$id", s"payment-$id", s"user-${id % USERS_COUNT + 1}", s"product-${id % PRODUCTS_COUNT + 1}", s"Country-${id % COUNTRIES_COUNT}", id % INSTANCES_COUNT + 1, price * (id % INSTANCES_COUNT + 1), weight * (id % INSTANCES_COUNT + 1), f"2023-02-${id % MAX_DAYS + 1}%02d 00:00:00", f"2023-03-${id % MAX_DAYS + 1}%02d 00:00:00", "COMPLETED")

  def createDynamicallyGeneratedSampleNotCompletedOrder(id: Int, price: Double, weight: Double): (String, String, String, String, String, Long, Double, Double, String, String, String) =
    (s"order-$id", s"payment-$id", s"user-${id % USERS_COUNT + 1}", s"product-${id % PRODUCTS_COUNT + 1}", s"Country-${id % COUNTRIES_COUNT}", id % INSTANCES_COUNT + 1, price * (id % INSTANCES_COUNT + 1), weight * (id % INSTANCES_COUNT + 1), f"2023-02-${id % MAX_DAYS + 1}%02d 00:00:00", s"", "NOT_COMPLETED")

  def createDynamicallyGeneratedSampleCompletedOrders(count: Int): Seq[(String, String, String, String, String, Long, Double, Double, String, String, String)] =
    (1 to count).map(i => createDynamicallyGeneratedSampleCompletedOrder(i, i * PRICE_INTERVAL % MAX_PRICE + PRICE_INTERVAL, i * WEIGHT_INTERVAL % MAX_WEIGHT + WEIGHT_INTERVAL)).toList

  def createDynamicallyGeneratedSampleNotCompletedOrders(count: Int): Seq[(String, String, String, String, String, Long, Double, Double, String, String, String)] =
    (1 to count).map(i => createDynamicallyGeneratedSampleNotCompletedOrder(i, i * PRICE_INTERVAL % MAX_PRICE + PRICE_INTERVAL, i * WEIGHT_INTERVAL % MAX_WEIGHT + WEIGHT_INTERVAL)).toList

  // STATICALLY GENERATED

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

  // CREATE DYNAMICALLY GENERATED DATAFRAMES

  def createDynamicallyGeneratedSampleUsersDF(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(createDynamicallyGeneratedSampleUsers(USERS_COUNT))
      .withColumnRenamed("_1", "UserId")
      .withColumnRenamed("_2", "FirstName")
      .withColumnRenamed("_3", "LastName")
      .withColumnRenamed("_4", "Country")
      .withColumnRenamed("_5", "City")
      .withColumnRenamed("_6", "PostalCode")
      .withColumnRenamed("_7", "Address")
  }

  def createDynamicallyGeneratedSampleCompletedOrdersDF(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(createDynamicallyGeneratedSampleCompletedOrders(COMPLETED_ORDERS_COUNT))
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

  def createDynamicallyGeneratedSampleNotCompletedOrdersDF(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(createDynamicallyGeneratedSampleNotCompletedOrders(NOT_COMPLETED_ORDERS_COUNT))
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

  def createDynamicallyGeneratedSampleProductsDF(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(createDynamicallyGeneratedSampleProducts(PRODUCTS_COUNT))
      .withColumnRenamed("_1", "ProductId")
      .withColumnRenamed("_2", "CategoryId")
      .withColumnRenamed("_3", "Name")
      .withColumnRenamed("_4", "Country")
      .withColumnRenamed("_5", "Price")
      .withColumnRenamed("_6", "Weight")
      .withColumnRenamed("_7", "MarketEntranceDate")
  }

  def createDynamicallyGeneratedSampleCategoriesDF(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(createDynamicallyGeneratedSampleCategories(CATEGORIES_COUNT))
      .withColumnRenamed("_1", "CategoryId")
      .withColumnRenamed("_2", "Name")
      .withColumnRenamed("_3", "Code")
      .withColumnRenamed("_4", "Country")
  }

  // CREATE STATICALLY GENERATED DATAFRAMES

  def createSampleUsersDF(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleUsers)
      .withColumnRenamed("_1", "UserId")
      .withColumnRenamed("_2", "FirstName")
      .withColumnRenamed("_3", "LastName")
      .withColumnRenamed("_4", "Country")
      .withColumnRenamed("_5", "City")
      .withColumnRenamed("_6", "PostalCode")
      .withColumnRenamed("_7", "Address")
  }

  def createSampleCompletedOrdersDF(implicit spark: SparkSession): DataFrame = {
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

  def createSampleNotCompletedOrdersDF(implicit spark: SparkSession): DataFrame = {
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

  def createSampleProductsDF(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleProducts)
      .withColumnRenamed("_1", "ProductId")
      .withColumnRenamed("_2", "CategoryId")
      .withColumnRenamed("_3", "Name")
      .withColumnRenamed("_4", "Country")
      .withColumnRenamed("_5", "Price")
      .withColumnRenamed("_6", "Weight")
      .withColumnRenamed("_7", "MarketEntranceDate")
  }

  def createSampleCategoriesDF(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleCategories)
      .withColumnRenamed("_1", "CategoryId")
      .withColumnRenamed("_2", "Name")
      .withColumnRenamed("_3", "Code")
      .withColumnRenamed("_4", "Country")
  }

  def createSampleCompletedPaymentsDF(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleCompletedPayments)
      .withColumnRenamed("_1", "PaymentId")
      .withColumnRenamed("_2", "Country")
      .withColumnRenamed("_3", "TotalValue")
      .withColumnRenamed("_4", "PaymentDeadline")
      .withColumnRenamed("_5", "PaymentCompletionDate")
      .withColumnRenamed("_6", "PaymentStatus")
  }

  def createSampleNotCompletedPaymentsDF(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(createSampleNotCompletedPayments)
      .withColumnRenamed("_1", "PaymentId")
      .withColumnRenamed("_2", "Country")
      .withColumnRenamed("_3", "TotalValue")
      .withColumnRenamed("_4", "PaymentDeadline")
      .withColumnRenamed("_5", "PaymentCompletionDate")
      .withColumnRenamed("_6", "PaymentStatus")
  }
}
