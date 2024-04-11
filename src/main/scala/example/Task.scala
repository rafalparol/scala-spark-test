package example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object Task {
  def main(args: Array[String]): Unit = {
    // CREATE SPARK SESSION

    def createSparkSession: SparkSession = SparkSession.builder
      .appName("Spark Scala Task")
      .getOrCreate()

    def createLocalSparkSession: SparkSession = {
      val ss = SparkSession.builder
        .appName("Spark Scala Task")
        .master("local")
        .getOrCreate()
      ss.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
      ss
    }

    val spark = createLocalSparkSession

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
    // usersDF.printSchema()

    val productsDF = createSampleProductsDF
    // productsDF.printSchema()

    val categoriesDF = createSampleCategoriesDF
    // categoriesDF.printSchema()

    val completedPaymentsDF = createSampleCompletedPaymentsDF
    // completedPaymentsDF.printSchema()

    val notCompletedPaymentsDF = createSampleNotCompletedPaymentsDF
    // notCompletedPaymentsDF.printSchema()

    val completedOrdersDF = createSampleCompletedOrdersDF
    // completedOrdersDF.printSchema()

    val notCompletedOrdersDF = createSampleNotCompletedOrdersDF
    // notCompletedOrdersDF.printSchema()

    // TASK 1

    // QUERY EXAMPLE: Find spendings (both already paid or not) of different users on products from different categories.

    // Naive approach.

    // Tests.

    // More optimized approach.

    // Tests.

    // TASK 2

    // QUERY EXAMPLE: Find spendings (both already paid or not) of different users on products from different categories.

    // Simple approach

    def transformationTask2WithSimpleApproach(
      notCompletedOrdersDF: DataFrame,
      completedOrdersDF: DataFrame,
      notCompletedPaymentsDF: DataFrame,
      completedPaymentsDF: DataFrame,
      usersDF: DataFrame,
      productsDF: DataFrame,
      categoriesDF: DataFrame
    ): DataFrame = {
      val ordersDF = notCompletedOrdersDF.union(completedOrdersDF)
      // ordersDF.printSchema()
      val paymentsDF = notCompletedPaymentsDF.union(completedPaymentsDF)
      // paymentsDF.printSchema()

      val usersJoinedWithOrdersDF = usersDF.join(ordersDF, usersDF.col("UserId") === ordersDF.col("UserId"), "left")
      // usersJoinedWithOrdersDF.printSchema()
      val usersJoinedWithOrdersAndProductsDF = usersJoinedWithOrdersDF.join(productsDF, ordersDF.col("ProductId") === productsDF.col("ProductId"), "left")
      // usersJoinedWithOrdersAndProductsDF.printSchema()
      val usersJoinedWithOrdersProductsAndCategoriesDF = usersJoinedWithOrdersAndProductsDF.join(categoriesDF, productsDF.col("CategoryId") === categoriesDF.col("CategoryId"), "left")
      // usersJoinedWithOrdersProductsAndCategoriesDF.printSchema()

      val groupedByUsersAndCategoriesDF = usersJoinedWithOrdersProductsAndCategoriesDF
        .groupBy(
          usersDF.col("UserId").as("ConsideredUserId"),
          productsDF.col("CategoryId").as("ConsideredCategoryId")
        )
        .agg(
          sum(ordersDF.col("TotalValue")).as("TotalSum")
        )
        .orderBy(asc("ConsideredUserId"), desc("ConsideredCategoryId"))

      // groupedByUsersAndCategoriesDF.explain()

      //   PHYSICAL PLAN WITH BROADCAST JOINS (FOR SMALL DATA SETS)
      //      == Physical Plan ==
      //        AdaptiveSparkPlan isFinalPlan=false
      //      +- Sort [ConsideredUserId#745 ASC NULLS FIRST, ConsideredCategoryId#746 DESC NULLS LAST], true, 0 // SORTING
      //        +- Exchange rangepartitioning(ConsideredUserId#745 ASC NULLS FIRST, ConsideredCategoryId#746 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=75]
      //          +- HashAggregate(keys=[UserId#14, CategoryId#92], functions=[sum(TotalValue#524)]) // GROUPING AND GLOBAL RESULTS OF SUM
      //            +- Exchange hashpartitioning(UserId#14, CategoryId#92, 200), ENSURE_REQUIREMENTS, [plan_id=72]
      //              +- HashAggregate(keys=[UserId#14, CategoryId#92], functions=[partial_sum(TotalValue#524)]) // GROUPING AND PARTIAL (LOCAL) RESULTS OF SUM
      //                +- Project [UserId#14, TotalValue#524, CategoryId#92]
      //                  +- BroadcastHashJoin [CategoryId#92], [CategoryId#148], LeftOuter, BuildRight, false // BROADCAST JOIN OF USERS ORDERS PRODUCTS AND CATEGORIES
      //                    :- Project [UserId#14, TotalValue#524, CategoryId#92]
      //                    :  +- BroadcastHashJoin [ProductId#488], [ProductId#84], LeftOuter, BuildRight, false // BROADCAST JOIN OF USERS ORDERS AND PRODUCTS
      //                    :     :- Project [UserId#14, ProductId#488, TotalValue#524]
      //                    :     :  +- BroadcastHashJoin [UserId#14], [UserId#476], LeftOuter, BuildRight, false // BROADCAST JOIN OF USERS AND ORDERS
      //                    :     :     :- LocalTableScan [UserId#14] // LOAD USERS
      //                    :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=59] // BROADCAST ORDERS
      //                    :     :        +- Union // DO UNION OF ORDERS
      //                    :     :           :- LocalTableScan [UserId#476, ProductId#488, TotalValue#524] // LOAD ORDERS
      //                    :     :           +- LocalTableScan [UserId#322, ProductId#334, TotalValue#370]   // LOAD ORDERS
      //                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=63] // BROADCAST PRODUCTS
      //                    :        +- LocalTableScan [ProductId#84, CategoryId#92] // LOAD PRODUCTS
      //                    +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=67] // BROADCAST CATEGORIES
      //                       +- LocalTableScan [CategoryId#148] // LOAD CATEGORIES

      //   PHYSICAL PLAN WITHOUT BROADCAST JOINS (FOR LARGE / PRODUCTION DATA SETS)
      //      == Physical Plan ==
      //        AdaptiveSparkPlan isFinalPlan=false
      //      +- Sort [ConsideredUserId#745 ASC NULLS FIRST, ConsideredCategoryId#746 DESC NULLS LAST], true, 0 // SORTING
      //        +- Exchange rangepartitioning(ConsideredUserId#745 ASC NULLS FIRST, ConsideredCategoryId#746 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=85]
      //          +- HashAggregate(keys=[UserId#14, CategoryId#92], functions=[sum(TotalValue#524)]) // GROUPING AND GLOBAL RESULTS OF SUM
      //            +- HashAggregate(keys=[UserId#14, CategoryId#92], functions=[partial_sum(TotalValue#524)]) // GROUPING AND PARTIAL (LOCAL) RESULTS OF SUM
      //              +- Project [UserId#14, TotalValue#524, CategoryId#92]
      //                +- SortMergeJoin [CategoryId#92], [CategoryId#148], LeftOuter // JOIN OF USERS ORDERS PRODUCTS AND CATEGORIES
      //                  :- Sort [CategoryId#92 ASC NULLS FIRST], false, 0 // PREPARATION FOR JOIN OF USERS ORDERS PRODUCTS AND CATEGORIES
      //                  :  +- Exchange hashpartitioning(CategoryId#92, 200), ENSURE_REQUIREMENTS, [plan_id=76]
      //                  :     +- Project [UserId#14, TotalValue#524, CategoryId#92]
      //                  :        +- SortMergeJoin [ProductId#488], [ProductId#84], LeftOuter // JOIN OF USERS ORDERS AND PRODUCTS
      //                  :           :- Sort [ProductId#488 ASC NULLS FIRST], false, 0 // PREPARATION FOR JOIN OF USERS ORDERS AND PRODUCTS
      //                  :           :  +- Exchange hashpartitioning(ProductId#488, 200), ENSURE_REQUIREMENTS, [plan_id=68]
      //                  :           :     +- Project [UserId#14, ProductId#488, TotalValue#524]
      //                  :           :        +- SortMergeJoin [UserId#14], [UserId#476], LeftOuter // JOIN OF USERS AND ORDERS
      //                  :           :           :- Sort [UserId#14 ASC NULLS FIRST], false, 0 // PREPARATION FOR JOIN OF USERS AND ORDERS
      //                  :           :           :  +- Exchange hashpartitioning(UserId#14, 200), ENSURE_REQUIREMENTS, [plan_id=60]
      //                  :           :           :     +- LocalTableScan [UserId#14] // LOAD USERS
      //                  :           :           +- Sort [UserId#476 ASC NULLS FIRST], false, 0 // PREPARATION FOR JOIN OF USERS AND ORDERS
      //                  :           :              +- Exchange hashpartitioning(UserId#476, 200), ENSURE_REQUIREMENTS, [plan_id=61]
      //                  :           :                 +- Union // DO UNION OF ORDERS
      //                  :           :                    :- LocalTableScan [UserId#476, ProductId#488, TotalValue#524] // LOAD ORDERS
      //                  :           :                    +- LocalTableScan [UserId#322, ProductId#334, TotalValue#370] // LOAD ORDERS
      //                  :           +- Sort [ProductId#84 ASC NULLS FIRST], false, 0 // PREPARATION FOR JOIN OF USERS ORDERS AND PRODUCTS
      //                  :              +- Exchange hashpartitioning(ProductId#84, 200), ENSURE_REQUIREMENTS, [plan_id=69]
      //                  :                 +- LocalTableScan [ProductId#84, CategoryId#92] // LOAD PRODUCTS
      //                  +- Sort [CategoryId#148 ASC NULLS FIRST], false, 0 // PREPARATION FOR JOIN OF USERS ORDERS PRODUCTS AND CATEGORIES
      //                    +- Exchange hashpartitioning(CategoryId#148, 200), ENSURE_REQUIREMENTS, [plan_id=77]
      //                      +- LocalTableScan [CategoryId#148] // LOAD CATEGORIES

      // groupedByUsersAndCategoriesDF.show()

      groupedByUsersAndCategoriesDF
    }

    val transformationTask2WithSimpleApproachDF = transformationTask2WithSimpleApproach(
      notCompletedOrdersDF,
      completedOrdersDF,
      notCompletedPaymentsDF,
      completedPaymentsDF,
      usersDF,
      productsDF,
      categoriesDF
    )

    // Tests.

    // Approach with repartitioning.

    def transformationTask2WithRepartitioningApproach(
      notCompletedOrdersDF: DataFrame,
      completedOrdersDF: DataFrame,
      notCompletedPaymentsDF: DataFrame,
      completedPaymentsDF: DataFrame,
      usersDF: DataFrame,
      productsDF: DataFrame,
      categoriesDF: DataFrame
    ): DataFrame = {
      // PREPROCESSING

      val preprocessedNotCompletedOrdersDF = notCompletedOrdersDF
        .repartition(col("UserId"))
        // .select(col("OrderId"), col("TotalValue"), col("UserId"), col("ProductId"))
      val preprocessedCompletedOrdersDF = completedOrdersDF
        .repartition(col("UserId"))
        // .select(col("OrderId"), col("TotalValue"), col("UserId"), col("ProductId"))
      val preprocessedUsersDF = usersDF
        .repartition(col("UserId"))
        // .select(col("UserId"))
      val preprocessedProductsDF = productsDF
        .repartition(col("CategoryId"))
        // .select(col("ProductId"), col("CategoryId"))
      val preprocessedCategoriesDF = categoriesDF
        .repartition(col("CategoryId"))
        // .select(col("CategoryId"))

      val preprocessedOrdersDF = preprocessedNotCompletedOrdersDF.union(preprocessedCompletedOrdersDF)
      // preprocessedOrdersDF.printSchema()

      // MAIN LOGIC

      val usersJoinedWithOrdersDF = preprocessedUsersDF.join(preprocessedOrdersDF, preprocessedUsersDF.col("UserId") === preprocessedOrdersDF.col("UserId"), "left")
      // usersJoinedWithOrdersDF.printSchema()
      val productsJoinedWithCategoriesDF = preprocessedProductsDF.join(preprocessedCategoriesDF, preprocessedProductsDF.col("CategoryId") === preprocessedCategoriesDF.col("CategoryId"), "left")
      // productsJoinedCategoriesDF.printSchema()

      val usersJoinedWithOrdersProductsAndCategoriesDF = usersJoinedWithOrdersDF.join(productsJoinedWithCategoriesDF, preprocessedOrdersDF.col("ProductId") === preprocessedProductsDF.col("ProductId"), "left")
      // usersJoinedWithOrdersProductsAndCategoriesDF.printSchema()

      val groupedByUsersAndCategoriesDF = usersJoinedWithOrdersProductsAndCategoriesDF
        .groupBy(
          preprocessedUsersDF.col("UserId").as("ConsideredUserId"),
          preprocessedProductsDF.col("CategoryId").as("ConsideredCategoryId")
        )
        .agg(
          sum(preprocessedOrdersDF.col("TotalValue")).as("TotalSum")
        )
        .orderBy(asc("ConsideredUserId"), desc("ConsideredCategoryId"))

      groupedByUsersAndCategoriesDF.explain()

      //    == Physical Plan ==
      //      AdaptiveSparkPlan isFinalPlan=false
      //    +- Sort [ConsideredUserId#711 ASC NULLS FIRST, ConsideredCategoryId#712 DESC NULLS LAST], true, 0 // SORTING
      //      +- Exchange rangepartitioning(ConsideredUserId#711 ASC NULLS FIRST, ConsideredCategoryId#712 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=103]
      //        +- HashAggregate(keys=[UserId#14, CategoryId#92], functions=[sum(TotalValue#524)]) // GROUPING AND GLOBAL RESULTS OF SUM
      //          +- Exchange hashpartitioning(UserId#14, CategoryId#92, 200), ENSURE_REQUIREMENTS, [plan_id=100]
      //            +- HashAggregate(keys=[UserId#14, CategoryId#92], functions=[partial_sum(TotalValue#524)]) // GROUPING AND PARTIAL (LOCAL) RESULTS OF SUM
      //              +- Project [UserId#14, TotalValue#524, CategoryId#92]
      //                +- SortMergeJoin [ProductId#488], [ProductId#84], LeftOuter // MAIN JOIN
      //                :- Sort [ProductId#488 ASC NULLS FIRST], false, 0 // PREPARATION FOR MAIN JOIN
      //                :  +- Exchange hashpartitioning(ProductId#488, 200), ENSURE_REQUIREMENTS, [plan_id=92]
      //                :     +- Project [UserId#14, ProductId#488, TotalValue#524]
      //                :        +- SortMergeJoin [UserId#14], [UserId#476], LeftOuter // JOIN OF ORDERS WITH USERS
      //                :           :- Sort [UserId#14 ASC NULLS FIRST], false, 0  // PREPARATION FOR JOIN OF ORDERS WITH USERS
      //                :           :  +- Exchange hashpartitioning(UserId#14, 200), REPARTITION_BY_COL, [plan_id=59] // REPARTITION USERS BY USER ID
      //                :           :     +- LocalTableScan [UserId#14] // LOAD USERS
      //                :           +- Sort [UserId#476 ASC NULLS FIRST], false, 0 // PREPARATION FOR JOIN OF ORDERS WITH USERS
      //                :              +- Exchange hashpartitioning(UserId#476, 200), ENSURE_REQUIREMENTS, [plan_id=80]
      //                :                 +- Union // DO UNION OF ORDERS
      //                :                    :- Exchange hashpartitioning(UserId#476, 200), REPARTITION_BY_COL, [plan_id=61] // REPARTITION ORDERS BY USER ID
      //                :                    :  +- LocalTableScan [UserId#476, ProductId#488, TotalValue#524] // LOAD ORDERS
      //                :                    +- Exchange hashpartitioning(UserId#322, 200), REPARTITION_BY_COL, [plan_id=63] // REPARTITION ORDERS BY USER ID
      //                :                       +- LocalTableScan [UserId#322, ProductId#334, TotalValue#370] // LOAD ORDERS
      //                +- Sort [ProductId#84 ASC NULLS FIRST], false, 0 // PREPARATION FOR MAIN JOIN
      //                  +- Exchange hashpartitioning(ProductId#84, 200), ENSURE_REQUIREMENTS, [plan_id=93]
      //                    +- Project [ProductId#84, CategoryId#92]
      //                      +- SortMergeJoin [CategoryId#92], [CategoryId#148], LeftOuter // JOIN OF PRODUCTS WITH CATEGORIES
      //                        :- Sort [CategoryId#92 ASC NULLS FIRST], false, 0 // PREPARATION FOR JOIN OF PRODUCTS WITH CATEGORIES
      //                        :  +- Exchange hashpartitioning(CategoryId#92, 200), REPARTITION_BY_COL, [plan_id=68] // REPARTITION PRODUCTS BY CATEGORY ID
      //                        :     +- LocalTableScan [ProductId#84, CategoryId#92] // LOAD PRODUCTS
      //                        +- Sort [CategoryId#148 ASC NULLS FIRST], false, 0
      //                           +- Exchange hashpartitioning(CategoryId#148, 200), REPARTITION_BY_COL, [plan_id=70] // REPARTITION CATEGORIES BY CATEGORY ID
      //                              +- LocalTableScan [CategoryId#148] // LOAD CATEGORIES

      groupedByUsersAndCategoriesDF.show()

      groupedByUsersAndCategoriesDF
    }

    val transformationTask2WithRepartitioningApproachDF = transformationTask2WithRepartitioningApproach(
      notCompletedOrdersDF,
      completedOrdersDF,
      notCompletedPaymentsDF,
      completedPaymentsDF,
      usersDF,
      productsDF,
      categoriesDF
    )

    // Tests.

    // spark.stop()
  }
}

