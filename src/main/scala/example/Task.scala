package example

import org.apache.spark.sql.{SparkSession}

object Task {
  def main(args: Array[String]): Unit = {
    // CREATE SPARK SESSION

    val spark = TaskInitialization.createLocalSparkSession

    // CREATE TEST DATAFRAMES

    val usersDF = TaskData.createSampleUsersDF(spark)
    val productsDF = TaskData.createSampleProductsDF(spark)
    val categoriesDF = TaskData.createSampleCategoriesDF(spark)
    val completedPaymentsDF = TaskData.createSampleCompletedPaymentsDF(spark)
    val notCompletedPaymentsDF = TaskData.createSampleNotCompletedPaymentsDF(spark)
    val completedOrdersDF = TaskData.createSampleCompletedOrdersDF(spark)
    val notCompletedOrdersDF = TaskData.createSampleNotCompletedOrdersDF(spark)

    // usersDF.printSchema()
    // productsDF.printSchema()
    // categoriesDF.printSchema()
    // completedPaymentsDF.printSchema()
    // notCompletedPaymentsDF.printSchema()
    // completedOrdersDF.printSchema()
    // notCompletedOrdersDF.printSchema()

    //  val transformationTask1WithSimpleApproachDF = transformationTask1WithSimpleApproach(
    //    notCompletedOrdersDF,
    //    completedOrdersDF,
    //    notCompletedPaymentsDF,
    //    completedPaymentsDF,
    //    usersDF,
    //    productsDF,
    //    categoriesDF
    //  )

    // Tests.

    // Approach with repartitioning.

    // Tests.

    //  val transformationTask1WithRepartitioningApproachDF = transformationTask1WithRepartitioningApproach(
    //    notCompletedOrdersDF,
    //    completedOrdersDF,
    //    notCompletedPaymentsDF,
    //    completedPaymentsDF,
    //    usersDF,
    //    productsDF,
    //    categoriesDF
    //  )

    // TASK 2

    // QUERY EXAMPLE: Find spendings (both already paid or not) of different users on products from different categories.

    // Simple approach

    //  val transformationTask2WithSimpleApproachDF = transformationTask2WithSimpleApproach(
    //    notCompletedOrdersDF,
    //    completedOrdersDF,
    //    notCompletedPaymentsDF,
    //    completedPaymentsDF,
    //    usersDF,
    //    productsDF,
    //    categoriesDF
    //  )

    // Tests.

    // Approach with repartitioning.

    //  val transformationTask2WithRepartitioningApproachDF = transformationTask2WithRepartitioningApproach(
    //    notCompletedOrdersDF,
    //    completedOrdersDF,
    //    notCompletedPaymentsDF,
    //    completedPaymentsDF,
    //    usersDF,
    //    productsDF,
    //    categoriesDF
    //  )

    // Tests.

    spark.stop()
  }
}

