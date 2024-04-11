package example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object Task {
  def main(args: Array[String]): Unit = {
    // CREATE SPARK SESSION

    val spark = SparkSession.builder
      .appName("Spark Scala Task")
      .master("local")
      .getOrCreate()

    // SCHEMAS (FOR DOCUMENTATION PURPOSES ONLY)

    // The model of the example:
    // 1. We have a catalogue of products.
    // 2. Users place orders buying products.
    // 3. Users pay for the orders by making payments.
    // 4. Products belong to categories.

    val categorySchema = StructType(Array(
      StructField("CategoryId", LongType),          // Category ID
      StructField("Name", StringType),              // Name of the category
      StructField("Code", StringType),              // Code of the category
      StructField("Country", StringType)            // Where this category is from
    ))

    val productSchema = StructType(Array(
      StructField("ProductId", LongType),           // Product ID
      StructField("Name", StringType),              // Name of the product
      StructField("Country", StringType),           // Where it is available
      StructField("Price", DoubleType),             // At what price
      StructField("Weight", DoubleType),            // How heavy it is
      StructField("MarketEntranceDate", StringType) // When it entered the market
    ))

    val orderSchema = StructType(Array(
      StructField("OrderId", LongType),               // Order ID
      StructField("PaymentId", LongType),             // Payment ID
      StructField("UserId", LongType),                // User ID
      StructField("ProductId", LongType),             // Product ID
      StructField("Country", StringType),             // Where the order came from
      StructField("Count", LongType),                 // How many instances
      StructField("TotalValue", DoubleType),          // Total value of the order
      StructField("TotalWeight", DoubleType),         // Total weight of the order
      StructField("OrderGenerationDate", StringType), // When the order was generated
      StructField("OrderCompletionDate", StringType), // When the order was completed
      StructField("Status", StringType)               // COMPLETED or NOT COMPLETED
    ))

    val userSchema = StructType(Array(
      StructField("UserId", LongType),        // User ID
      StructField("FirstName", StringType),   // First name of the user
      StructField("LastName", StringType),    // Last name of the user
      StructField("Country", StringType),     // Where she / he comes from
      StructField("City", StringType),        // Where she / he lives
      StructField("PostalCode", StringType),  // Her / his postal code
      StructField("Address", StringType)      // Her / his full address
    ))

    val paymentSchema = StructType(Array(
      StructField("PaymentId", LongType),               // Payment ID
      StructField("Country", StringType),               // Where the payment belongs to
      StructField("TotalValue", DoubleType),            // Total amount to pay
      StructField("PaymentDeadline", StringType),       // When it needs to be paid
      StructField("PaymentCompletionDate", StringType), // When it was paid
      StructField("PaymentStatus", StringType)          // COMPLETED or NOT COMPLETED
    ))

    // TEST DATA

    def createSampleUsers: Seq[(String, String, String, String, String, String, String)] = List(
      ("user-01", "Anne", "Anderson", "USA", "Boston", "02138", "19 Ware St, Cambridge, MA 02138, USA"),
      ("user-02", "Tommy", "Harada", "Japan", "Ebina", "243-0402", "555-1 Kashiwagaya, Ebina, Kanagawa 243-0402, Japan"),
      ("user-03", "Stephane", "Moreau", "France", "Paris", "75003", "14 R. des Minimes, 75003 Paris, France")
    )

    def createSampleUsersDF: DataFrame = {
      spark.createDataFrame(createSampleUsers)
        .withColumnRenamed("_1", "UserId")
        .withColumnRenamed("_2", "FirstName")
        .withColumnRenamed("_3", "LastName")
        .withColumnRenamed("_4", "Country")
        .withColumnRenamed("_5", "City")
        .withColumnRenamed("_6", "PostalCode")
        .withColumnRenamed("_7", "Address")
    }

    def createSampleCompletedOrders: Seq[(String, String, String, String, String, Long, Double, Double, String, String, String)] = Seq(
      ("order-01", "payment-01", "user-01", "laptop-01", "USA", 1L, 100.0, 20.0, "2023-02-01 00:00:00", "2023-02-09 00:00:00", "COMPLETED"),
      ("order-02", "payment-02", "user-02", "laptop-02", "Japan", 2L, 400.0, 80.0, "2023-03-01 00:00:00", "2023-03-09 00:00:00", "COMPLETED"),
      ("order-03", "payment-03", "user-03", "laptop-03", "France", 1L, 300.0, 30.0, "2023-04-01 00:00:00", "2023-04-09 00:00:00", "COMPLETED"),
      ("order-04", "payment-04", "user-01", "laptop-05", "USA", 2L, 500.0, 20.0, "2023-05-01 00:00:00", "2023-05-09 00:00:00", "COMPLETED")
    )

    def createSampleCompletedOrdersDF: DataFrame = {
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

    def createSampleNotCompletedOrders: Seq[(String, String, String, String, String, Long, Double, Double, String, String, String)] = Seq(
      ("order-05", "payment-05", "user-02", "laptop-06", "Japan", 1L, 350.0, 40.0, "2023-07-01 00:00:00", "", "NOT COMPLETED"),
      ("order-06", "payment-06", "user-03", "laptop-07", "France", 2L, 200.0, 160.0, "2023-07-01 00:00:00", "", "NOT COMPLETED")
    )

    def createSampleNotCompletedOrdersDF: DataFrame = {
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

    def createSampleProductsDF: DataFrame = {
      spark.createDataFrame(createSampleProducts)
        .withColumnRenamed("_1", "ProductId")
        .withColumnRenamed("_2", "CategoryId")
        .withColumnRenamed("_3", "Name")
        .withColumnRenamed("_4", "Country")
        .withColumnRenamed("_5", "Price")
        .withColumnRenamed("_6", "Weight")
        .withColumnRenamed("_7", "MarketEntranceDate")
    }

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

    def createSampleCategoriesDF: DataFrame = {
      spark.createDataFrame(createSampleCategories)
        .withColumnRenamed("_1", "CategoryId")
        .withColumnRenamed("_2", "Name")
        .withColumnRenamed("_3", "Code")
        .withColumnRenamed("_4", "Country")
    }

    def createSampleCompletedPayments: Seq[(String, String, Double, String, String, String)] = Seq(
      ("payment-01", "USA", 100.0, "2023-02-15 00:00:00", "2023-02-08 00:00:00", "COMPLETED"),
      ("payment-02", "Japan", 400.0, "2023-03-15 00:00:00", "2023-03-08 00:00:00", "COMPLETED"),
      ("payment-03", "France", 300.0, "2023-04-15 00:00:00", "2023-04-08 00:00:00", "COMPLETED"),
      ("payment-04", "USA", 500.0, "2023-05-15 00:00:00", "2023-05-08 00:00:00", "COMPLETED")
    )

    def createSampleCompletedPaymentsDF: DataFrame = {
      spark.createDataFrame(createSampleCompletedPayments)
        .withColumnRenamed("_1", "PaymentId")
        .withColumnRenamed("_2", "Country")
        .withColumnRenamed("_3", "TotalValue")
        .withColumnRenamed("_4", "PaymentDeadline")
        .withColumnRenamed("_5", "PaymentCompletionDate")
        .withColumnRenamed("_6", "PaymentStatus")
    }

    def createSampleNotCompletedPayments: Seq[(String, String, Double, String, String, String)] = Seq(
      ("payment-05", "Japan", 350.0, "2023-07-15 00:00:00", "", "NOT COMPLETED"),
      ("payment-06", "France", 200.0, "2023-07-15 00:00:00", "", "NOT COMPLETED")
    )

    def createSampleNotCompletedPaymentsDF: DataFrame = {
      spark.createDataFrame(createSampleNotCompletedPayments)
        .withColumnRenamed("_1", "PaymentId")
        .withColumnRenamed("_2", "Country")
        .withColumnRenamed("_3", "TotalValue")
        .withColumnRenamed("_4", "PaymentDeadline")
        .withColumnRenamed("_5", "PaymentCompletionDate")
        .withColumnRenamed("_6", "PaymentStatus")
    }

    // CREATE DATAFRAMES

    val usersDF = createSampleUsersDF
    usersDF.printSchema()

    val productsDF = createSampleProductsDF
    productsDF.printSchema()

    val categoriesDF = createSampleCategoriesDF
    categoriesDF.printSchema()

    val completedPaymentsDF = createSampleCompletedPaymentsDF
    completedPaymentsDF.printSchema()

    val notCompletedPaymentsDF = createSampleNotCompletedPaymentsDF
    notCompletedPaymentsDF.printSchema()

    val completedOrdersDF = createSampleCompletedOrdersDF
    completedOrdersDF.printSchema()

    val notCompletedOrdersDF = createSampleNotCompletedOrdersDF
    notCompletedOrdersDF.printSchema()

    // TASK 1

    // Naive approach.

    // Tests.

    // More optimized approach.

    // Tests.

    // TASK 2

    //

    // Naive approach

    // Tests.

    // More optimized approach.

    // Tests.

    spark.stop()
  }
}

