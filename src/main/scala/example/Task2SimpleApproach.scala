package example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, desc, sum}

object Task2SimpleApproach {
  // QUERY: Find spendings (both already paid or not) of different users on products from different categories.

  def transformationTask2WithSimpleApproach(
    notCompletedOrdersDF: DataFrame,
    completedOrdersDF: DataFrame,
    usersDF: DataFrame,
    productsDF: DataFrame,
    categoriesDF: DataFrame
  ): DataFrame = {
    val ordersDF = notCompletedOrdersDF
      .union(completedOrdersDF)

    val usersJoinedWithOrdersDF = usersDF.join(ordersDF, usersDF.col("UserId") === ordersDF.col("UserId"), "left")
    val usersJoinedWithOrdersAndProductsDF = usersJoinedWithOrdersDF.join(productsDF, ordersDF.col("ProductId") === productsDF.col("ProductId"), "left")
    val usersJoinedWithOrdersProductsAndCategoriesDF = usersJoinedWithOrdersAndProductsDF.join(categoriesDF, productsDF.col("CategoryId") === categoriesDF.col("CategoryId"), "left")

    val groupedByUsersAndCategoriesDF = usersJoinedWithOrdersProductsAndCategoriesDF
      .groupBy(
        usersDF.col("UserId").as("ConsideredUserId"),
        productsDF.col("CategoryId").as("ConsideredCategoryId")
      )
      .agg(
        sum(ordersDF.col("TotalValue")).as("TotalSum")
      )
      .orderBy(asc("ConsideredUserId"), desc("TotalSum"))

    // groupedByUsersAndCategoriesDF.explain()

    // WITHOUT "COMPLICATIONS"

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

    // WITHOUT "COMPLICATIONS"

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

    groupedByUsersAndCategoriesDF
  }
}
