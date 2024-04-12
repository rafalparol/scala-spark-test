package example

import org.apache.spark.sql.{DataFrame, SparkSession}

object Task1SimpleApproach {
  // QUERY: Find spendings (both already paid or not) of different users on products from different categories.

  def transformationTask1WithSimpleApproach(
    spark: SparkSession,
    notCompletedOrdersDF: DataFrame,
    completedOrdersDF: DataFrame,
    notCompletedPaymentsDF: DataFrame,
    completedPaymentsDF: DataFrame,
    usersDF: DataFrame,
    productsDF: DataFrame,
    categoriesDF: DataFrame
  ): DataFrame = {
    usersDF
      .createOrReplaceTempView("users")
    productsDF
      .createOrReplaceTempView("products")
    categoriesDF
      .createOrReplaceTempView("categories")
//  completedPaymentsDF
//    .createOrReplaceTempView("completed_payments")
//  notCompletedPaymentsDF
//    .createOrReplaceTempView("not_completed_payments")
//  completedOrdersDF
//    .createOrReplaceTempView("completed_orders")
//  notCompletedOrdersDF
//    .createOrReplaceTempView("not_completed_orders")
    completedOrdersDF
      .union(notCompletedOrdersDF)
      .createOrReplaceTempView("orders")

    val groupedByUsersAndCategoriesDF = spark.sql("SELECT users.UserId AS ConsideredUserId, categories.CategoryId AS ConsideredCategoryId, SUM(orders.TotalValue) AS TotalSum FROM users LEFT JOIN orders ON users.UserId = orders.UserId LEFT JOIN products ON orders.ProductId = products.ProductId LEFT JOIN categories ON products.CategoryId = categories.CategoryId GROUP BY users.UserId, categories.CategoryId ORDER BY ConsideredUserId ASC, TotalSum DESC")

    // groupedByUsersAndCategoriesDF.explain()

    //  == Physical Plan ==
    //    AdaptiveSparkPlan isFinalPlan=false
    //  +- Sort [ConsideredUserId#595 ASC NULLS FIRST, TotalSum#597 DESC NULLS LAST], true, 0
    //  +- Exchange rangepartitioning(ConsideredUserId#595 ASC NULLS FIRST, TotalSum#597 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=99]
    //    +- HashAggregate(keys=[UserId#14, CategoryId#148], functions=[sum(TotalValue#370)])
    //      +- Exchange hashpartitioning(UserId#14, CategoryId#148, 200), ENSURE_REQUIREMENTS, [plan_id=96]
    //        +- HashAggregate(keys=[UserId#14, CategoryId#148], functions=[partial_sum(TotalValue#370)])
    //          +- Project [UserId#14, TotalValue#370, CategoryId#148]
    //            +- SortMergeJoin [CategoryId#92], [CategoryId#148], LeftOuter
    //            :- Sort [CategoryId#92 ASC NULLS FIRST], false, 0
    //            :  +- Exchange hashpartitioning(CategoryId#92, 200), ENSURE_REQUIREMENTS, [plan_id=88]
    //            :     +- Project [UserId#14, TotalValue#370, CategoryId#92]
    //            :        +- SortMergeJoin [ProductId#334], [ProductId#84], LeftOuter
    //            :           :- Sort [ProductId#334 ASC NULLS FIRST], false, 0
    //            :           :  +- Exchange hashpartitioning(ProductId#334, 200), ENSURE_REQUIREMENTS, [plan_id=80]
    //            :           :     +- Project [UserId#14, ProductId#334, TotalValue#370]
    //            :           :        +- SortMergeJoin [UserId#14], [UserId#322], LeftOuter
    //            :           :           :- Sort [UserId#14 ASC NULLS FIRST], false, 0
    //            :           :           :  +- Exchange hashpartitioning(UserId#14, 200), ENSURE_REQUIREMENTS, [plan_id=72]
    //            :           :           :     +- LocalTableScan [UserId#14]
    //            :           :           +- Sort [UserId#322 ASC NULLS FIRST], false, 0
    //            :           :              +- Exchange hashpartitioning(UserId#322, 200), ENSURE_REQUIREMENTS, [plan_id=73]
    //            :           :                 +- Union
    //            :           :                    :- LocalTableScan [UserId#322, ProductId#334, TotalValue#370]
    //            :           :                    +- LocalTableScan [UserId#476, ProductId#488, TotalValue#524]
    //            :           +- Sort [ProductId#84 ASC NULLS FIRST], false, 0
    //            :              +- Exchange hashpartitioning(ProductId#84, 200), ENSURE_REQUIREMENTS, [plan_id=81]
    //            :                 +- LocalTableScan [ProductId#84, CategoryId#92]
    //            +- Sort [CategoryId#148 ASC NULLS FIRST], false, 0
    //               +- Exchange hashpartitioning(CategoryId#148, 200), ENSURE_REQUIREMENTS, [plan_id=89]
    //                  +- LocalTableScan [CategoryId#148]

    // groupedByUsersAndCategoriesDF.show()

    groupedByUsersAndCategoriesDF
  }
}
