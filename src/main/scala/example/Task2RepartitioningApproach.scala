package example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, desc, sum}

/** Represents an algorithm for Task 2, repartition-based approach.
 *  QUERY: Find spendings (both already paid or not) of different users on products from different categories.
 */

object Task2RepartitioningApproach {
  // QUERY: Find spendings (both already paid or not) of different users on products from different categories.

  /** An algorithm for Task 2, repartition-based approach
   *
   * @param notCompletedOrdersDF set of not completed orders
   * @param completedOrdersDF set of completed orders
   * @param usersDF set of users
   * @param productsDF set of products
   * @param categoriesDF set of categories
   * @return Spendings (both already paid or not) of different users on products from different categories.
   */
  def transformationTask2WithRepartitioningApproach(
    notCompletedOrdersDF: DataFrame,
    completedOrdersDF: DataFrame,
    usersDF: DataFrame,
    productsDF: DataFrame,
    categoriesDF: DataFrame
  ): DataFrame = {
    val preprocessedNotCompletedOrdersDF = notCompletedOrdersDF
      .repartition(col("UserId"))

    val preprocessedCompletedOrdersDF = completedOrdersDF
      .repartition(col("UserId"))

    val preprocessedUsersDF = usersDF
      .repartition(col("UserId"))

    val preprocessedProductsDF = productsDF
      .repartition(col("CategoryId"))

    val preprocessedCategoriesDF = categoriesDF
      .repartition(col("CategoryId"))

    val preprocessedOrdersDF = preprocessedNotCompletedOrdersDF.union(preprocessedCompletedOrdersDF)

    val usersJoinedWithOrdersDF = preprocessedUsersDF.join(preprocessedOrdersDF, preprocessedUsersDF.col("UserId") === preprocessedOrdersDF.col("UserId"), "left")
    val productsJoinedWithCategoriesDF = preprocessedProductsDF.join(preprocessedCategoriesDF, preprocessedProductsDF.col("CategoryId") === preprocessedCategoriesDF.col("CategoryId"), "left")
    val usersJoinedWithOrdersProductsAndCategoriesDF = usersJoinedWithOrdersDF.join(productsJoinedWithCategoriesDF, preprocessedOrdersDF.col("ProductId") === preprocessedProductsDF.col("ProductId"), "left")

    val groupedByUsersAndCategoriesDF = usersJoinedWithOrdersProductsAndCategoriesDF
      .groupBy(
        preprocessedUsersDF.col("UserId").as("ConsideredUserId"),
        preprocessedProductsDF.col("CategoryId").as("ConsideredCategoryId")
      )
      .agg(
        sum(preprocessedOrdersDF.col("TotalValue")).as("TotalSum")
      )
      .orderBy(asc("ConsideredUserId"), desc("TotalSum"))

    // groupedByUsersAndCategoriesDF.explain()

    // WITHOUT "COMPLICATIONS"

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

    groupedByUsersAndCategoriesDF
  }
}
