package example

import example.Task1RepartitioningApproach.transformationTask1WithRepartitioningApproach
import example.Task1SimpleApproach.transformationTask1WithSimpleApproach
import example.Task2RepartitioningApproach.transformationTask2WithRepartitioningApproach
import example.Task2SimpleApproach.transformationTask2WithSimpleApproach
import org.apache.spark.sql.SparkSession

object TaskMain {
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

    val transformationTask1WithSimpleApproachDF = transformationTask1WithSimpleApproach(
      spark,
      notCompletedOrdersDF,
      completedOrdersDF,
      notCompletedPaymentsDF,
      completedPaymentsDF,
      usersDF,
      productsDF,
      categoriesDF
    )

    transformationTask1WithSimpleApproachDF.show()

    // Approach with repartitioning.

    //  val transformationTask1WithRepartitioningApproachDF = transformationTask1WithRepartitioningApproach(
    //    spark,
    //    notCompletedOrdersDF,
    //    completedOrdersDF,
    //    notCompletedPaymentsDF,
    //    completedPaymentsDF,
    //    usersDF,
    //    productsDF,
    //    categoriesDF
    //  )

    // TASK 2

    // Simple approach

    //  val transformationTask2WithSimpleApproachDF = transformationTask2WithSimpleApproach(
    //    spark,
    //    notCompletedOrdersDF,
    //    completedOrdersDF,
    //    notCompletedPaymentsDF,
    //    completedPaymentsDF,
    //    usersDF,
    //    productsDF,
    //    categoriesDF
    //  )

    // Approach with repartitioning.

    //  val transformationTask2WithRepartitioningApproachDF = transformationTask2WithRepartitioningApproach(
    //    spark,
    //    notCompletedOrdersDF,
    //    completedOrdersDF,
    //    notCompletedPaymentsDF,
    //    completedPaymentsDF,
    //    usersDF,
    //    productsDF,
    //    categoriesDF
    //  )

    spark.stop()
  }
}

