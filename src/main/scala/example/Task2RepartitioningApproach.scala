package example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{asc, col, desc, sum}

object Task2RepartitioningApproach {
  def transformationTask2WithRepartitioningApproach(
    spark: SparkSession,
    notCompletedOrdersDF: DataFrame,
    completedOrdersDF: DataFrame,
    notCompletedPaymentsDF: DataFrame,
    completedPaymentsDF: DataFrame,
    usersDF: DataFrame,
    productsDF: DataFrame,
    categoriesDF: DataFrame
  ): DataFrame = {
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

    // groupedByUsersAndCategoriesDF.explain()

    //  == Physical Plan ==
    //    AdaptiveSparkPlan isFinalPlan=false
    //  +- Sort [ConsideredUserId#711 ASC NULLS FIRST, ConsideredCategoryId#712 DESC NULLS LAST], true, 0 // SORTING
    //    +- Exchange rangepartitioning(ConsideredUserId#711 ASC NULLS FIRST, ConsideredCategoryId#712 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=103]
    //      +- HashAggregate(keys=[UserId#14, CategoryId#92], functions=[sum(TotalValue#524)]) // GROUPING AND GLOBAL RESULTS OF SUM
    //        +- Exchange hashpartitioning(UserId#14, CategoryId#92, 200), ENSURE_REQUIREMENTS, [plan_id=100]
    //          +- HashAggregate(keys=[UserId#14, CategoryId#92], functions=[partial_sum(TotalValue#524)]) // GROUPING AND PARTIAL (LOCAL) RESULTS OF SUM
    //            +- Project [UserId#14, TotalValue#524, CategoryId#92]
    //              +- SortMergeJoin [ProductId#488], [ProductId#84], LeftOuter // MAIN JOIN
    //              :- Sort [ProductId#488 ASC NULLS FIRST], false, 0 // PREPARATION FOR MAIN JOIN
    //              :  +- Exchange hashpartitioning(ProductId#488, 200), ENSURE_REQUIREMENTS, [plan_id=92]
    //              :     +- Project [UserId#14, ProductId#488, TotalValue#524]
    //              :        +- SortMergeJoin [UserId#14], [UserId#476], LeftOuter // JOIN OF ORDERS WITH USERS
    //              :           :- Sort [UserId#14 ASC NULLS FIRST], false, 0  // PREPARATION FOR JOIN OF ORDERS WITH USERS
    //              :           :  +- Exchange hashpartitioning(UserId#14, 200), REPARTITION_BY_COL, [plan_id=59] // REPARTITION USERS BY USER ID
    //              :           :     +- LocalTableScan [UserId#14] // LOAD USERS
    //              :           +- Sort [UserId#476 ASC NULLS FIRST], false, 0 // PREPARATION FOR JOIN OF ORDERS WITH USERS
    //              :              +- Exchange hashpartitioning(UserId#476, 200), ENSURE_REQUIREMENTS, [plan_id=80]
    //              :                 +- Union // DO UNION OF ORDERS
    //              :                    :- Exchange hashpartitioning(UserId#476, 200), REPARTITION_BY_COL, [plan_id=61] // REPARTITION ORDERS BY USER ID
    //              :                    :  +- LocalTableScan [UserId#476, ProductId#488, TotalValue#524] // LOAD ORDERS
    //              :                    +- Exchange hashpartitioning(UserId#322, 200), REPARTITION_BY_COL, [plan_id=63] // REPARTITION ORDERS BY USER ID
    //              :                       +- LocalTableScan [UserId#322, ProductId#334, TotalValue#370] // LOAD ORDERS
    //              +- Sort [ProductId#84 ASC NULLS FIRST], false, 0 // PREPARATION FOR MAIN JOIN
    //                +- Exchange hashpartitioning(ProductId#84, 200), ENSURE_REQUIREMENTS, [plan_id=93]
    //                  +- Project [ProductId#84, CategoryId#92]
    //                    +- SortMergeJoin [CategoryId#92], [CategoryId#148], LeftOuter // JOIN OF PRODUCTS WITH CATEGORIES
    //                      :- Sort [CategoryId#92 ASC NULLS FIRST], false, 0 // PREPARATION FOR JOIN OF PRODUCTS WITH CATEGORIES
    //                      :  +- Exchange hashpartitioning(CategoryId#92, 200), REPARTITION_BY_COL, [plan_id=68] // REPARTITION PRODUCTS BY CATEGORY ID
    //                      :     +- LocalTableScan [ProductId#84, CategoryId#92] // LOAD PRODUCTS
    //                      +- Sort [CategoryId#148 ASC NULLS FIRST], false, 0
    //                         +- Exchange hashpartitioning(CategoryId#148, 200), REPARTITION_BY_COL, [plan_id=70] // REPARTITION CATEGORIES BY CATEGORY ID
    //                            +- LocalTableScan [CategoryId#148] // LOAD CATEGORIES

    // PERFORMANCE TEST

    //  val COMPLETED_ORDERS_COUNT = 50000
    //  val NOT_COMPLETED_ORDERS_COUNT = 50000
    //  val USERS_COUNT = 10000
    //  val INSTANCES_COUNT = 5
    //  val COUNTRIES_COUNT = 10
    //  val MANUFACTURERS_COUNT = 20
    //  val PRODUCTS_COUNT = 1000
    //  val CATEGORIES_COUNT = 100
    //
    //  val PRICE_INTERVAL = 100
    //  val MAX_PRICE = 500
    //
    //  val WEIGHT_INTERVAL = 20
    //  val MAX_WEIGHT = 100
    //
    //  val MAX_DAYS = 28

    // Around 6s, the effort is moved from the end to the beginning of computations. Repartitioning is approximately as costly as a shuffle.
    // For a bigger number of joins using the same partitioning columns it can pays off - it will be done once at the beginning.

    // groupedByUsersAndCategoriesDF.show()

    groupedByUsersAndCategoriesDF
  }
}
