package example

import org.apache.spark.sql.functions.{asc, col, concat, desc, lit, split, sum, to_date, upper}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task2SimpleApproachWithComplications {
  // QUERY: Find spendings (both already paid or not) of different users on products from different categories.
  //        Do not consider orders older than a specific day. Each user ID should be accompanied by first name and last name in uppercase.

  def transformationTask2WithSimpleApproachWithComplications(
    spark: SparkSession,
    notCompletedOrdersDF: DataFrame,
    completedOrdersDF: DataFrame,
    notCompletedPaymentsDF: DataFrame,
    completedPaymentsDF: DataFrame,
    usersDF: DataFrame,
    productsDF: DataFrame,
    categoriesDF: DataFrame,
    startDate: String
  ): DataFrame = {
    val ordersDF = notCompletedOrdersDF
      .union(completedOrdersDF)
    // ordersDF.printSchema()
    // val paymentsDF = notCompletedPaymentsDF
    //   .union(completedPaymentsDF)
    // paymentsDF.printSchema()

    val modifiedUsersDF = usersDF
      .withColumn("FullNameInUpperCase", concat(upper(usersDF.col("FirstName")), lit(" "), upper(usersDF.col("LastName"))))

    val modifiedOrdersDF = ordersDF
      .withColumn("ParsedOrderGenerationDate", to_date(split(ordersDF.col("OrderGenerationDate"), " ")(0), "yyyy-MM-dd"))
      .filter(col("ParsedOrderGenerationDate") > startDate)

    val usersJoinedWithOrdersDF = modifiedUsersDF.join(modifiedOrdersDF, modifiedUsersDF.col("UserId") === modifiedOrdersDF.col("UserId"), "left")
    // usersJoinedWithOrdersDF.printSchema()
    val usersJoinedWithOrdersAndProductsDF = usersJoinedWithOrdersDF.join(productsDF, modifiedOrdersDF.col("ProductId") === productsDF.col("ProductId"), "left")
    // usersJoinedWithOrdersAndProductsDF.printSchema()
    val usersJoinedWithOrdersProductsAndCategoriesDF = usersJoinedWithOrdersAndProductsDF.join(categoriesDF, productsDF.col("CategoryId") === categoriesDF.col("CategoryId"), "left")
    // usersJoinedWithOrdersProductsAndCategoriesDF.printSchema()

    val groupedByUsersAndCategoriesDF = usersJoinedWithOrdersProductsAndCategoriesDF
      .groupBy(
        usersDF.col("UserId").as("ConsideredUserId"),
        modifiedUsersDF.col("FullNameInUpperCase").as("ConsideredFullNameInUpperCase"),
        productsDF.col("CategoryId").as("ConsideredCategoryId")
      )
      .agg(
        sum(modifiedOrdersDF.col("TotalValue")).as("TotalSum")
      )
      .orderBy(asc("ConsideredUserId"), desc("TotalSum"))

    // groupedByUsersAndCategoriesDF.explain()

    // WITH "COMPLICATIONS"

    //  == Physical Plan ==
    //    AdaptiveSparkPlan isFinalPlan=false
    //  +- Sort [ConsideredUserId#773 ASC NULLS FIRST, ConsideredCategoryId#775 DESC NULLS LAST], true, 0
    //    +- Exchange rangepartitioning(ConsideredUserId#773 ASC NULLS FIRST, ConsideredCategoryId#775 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=85]
    //      +- HashAggregate(keys=[UserId#14, FullNameInUpperCase#595, CategoryId#92], functions=[sum(TotalValue#524)])
    //        +- HashAggregate(keys=[UserId#14, FullNameInUpperCase#595, CategoryId#92], functions=[partial_sum(TotalValue#524)])
    //          +- Project [UserId#14, FullNameInUpperCase#595, TotalValue#524, CategoryId#92]
    //            +- SortMergeJoin [CategoryId#92], [CategoryId#148], LeftOuter
    //            :- Sort [CategoryId#92 ASC NULLS FIRST], false, 0
    //            :  +- Exchange hashpartitioning(CategoryId#92, 200), ENSURE_REQUIREMENTS, [plan_id=76]
    //            :     +- Project [UserId#14, FullNameInUpperCase#595, TotalValue#524, CategoryId#92]
    //            :        +- SortMergeJoin [ProductId#488], [ProductId#84], LeftOuter
    //            :           :- Sort [ProductId#488 ASC NULLS FIRST], false, 0
    //            :           :  +- Exchange hashpartitioning(ProductId#488, 200), ENSURE_REQUIREMENTS, [plan_id=68]
    //            :           :     +- Project [UserId#14, FullNameInUpperCase#595, ProductId#488, TotalValue#524]
    //            :           :        +- SortMergeJoin [UserId#14], [UserId#476], LeftOuter
    //            :           :           :- Sort [UserId#14 ASC NULLS FIRST], false, 0
    //            :           :           :  +- Exchange hashpartitioning(UserId#14, 200), ENSURE_REQUIREMENTS, [plan_id=60]
    //            :           :           :     +- LocalTableScan [UserId#14, FullNameInUpperCase#595]
    //            :           :           +- Sort [UserId#476 ASC NULLS FIRST], false, 0
    //            :           :              +- Exchange hashpartitioning(UserId#476, 200), ENSURE_REQUIREMENTS, [plan_id=61]
    //            :           :                 +- Union
    //            :           :                    :- LocalTableScan [UserId#476, ProductId#488, TotalValue#524]
    //            :           :                    +- LocalTableScan [UserId#322, ProductId#334, TotalValue#370]
    //            :           +- Sort [ProductId#84 ASC NULLS FIRST], false, 0
    //            :              +- Exchange hashpartitioning(ProductId#84, 200), ENSURE_REQUIREMENTS, [plan_id=69]
    //            :                 +- LocalTableScan [ProductId#84, CategoryId#92]
    //            +- Sort [CategoryId#148 ASC NULLS FIRST], false, 0
    //               +- Exchange hashpartitioning(CategoryId#148, 200), ENSURE_REQUIREMENTS, [plan_id=77]
    //                  +- LocalTableScan [CategoryId#148]

    // groupedByUsersAndCategoriesDF.show()

    groupedByUsersAndCategoriesDF
  }
}
