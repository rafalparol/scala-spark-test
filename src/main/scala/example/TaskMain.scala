package example

import example.Task1RepartitioningApproach.transformationTask1WithRepartitioningApproach
import example.Task1SimpleApproach.transformationTask1WithSimpleApproach
import example.Task1SimpleApproachWithComplications.transformationTask1WithSimpleApproachWithComplications
import example.Task2RepartitioningApproach.transformationTask2WithRepartitioningApproach
import example.Task2SimpleApproach.transformationTask2WithSimpleApproach
import example.Task2SimpleApproachWithComplications.transformationTask2WithSimpleApproachWithComplications
import org.apache.spark.sql.SparkSession

/** Represents main class - for experimenting and Production flow.
 *
 */

object TaskMain {
  def main(args: Array[String]): Unit = {
    // CREATE SPARK SESSION

    implicit val spark: SparkSession = TaskInitialization.createSparkSession

    // CREATE TEST DATAFRAMES

    // LOAD TABLES / PARQUET FILES

    val usersDF = TaskDatabase.loadSampleUsersDF
    val productsDF = TaskDatabase.loadSampleProductsDF
    val categoriesDF = TaskDatabase.loadSampleCategoriesDF
    val completedOrdersDF = TaskDatabase.loadSampleCompletedOrdersDF
    val notCompletedOrdersDF = TaskDatabase.loadSampleNotCompletedOrdersDF

    // SCHEMAS

    // usersDF.printSchema()
    // productsDF.printSchema()
    // categoriesDF.printSchema()
    // completedPaymentsDF.printSchema()
    // notCompletedPaymentsDF.printSchema()
    // completedOrdersDF.printSchema()
    // notCompletedOrdersDF.printSchema()

    // TASK 1

    // Simple approach.

    val transformationTask1WithSimpleApproachDF = transformationTask1WithSimpleApproach(
      notCompletedOrdersDF,
      completedOrdersDF,
      usersDF,
      productsDF,
      categoriesDF
    )

    transformationTask1WithSimpleApproachDF.show()

    // Approach with repartitioning.

    val transformationTask1WithRepartitioningApproachDF = transformationTask1WithRepartitioningApproach(
      notCompletedOrdersDF,
      completedOrdersDF,
      usersDF,
      productsDF,
      categoriesDF
    )

    transformationTask1WithRepartitioningApproachDF.show()

    // Simple approach with "complications".

    val transformationTask1WithSimpleApproachWithComplicationsDF = transformationTask1WithSimpleApproachWithComplications(
      notCompletedOrdersDF,
      completedOrdersDF,
      usersDF,
      productsDF,
      categoriesDF,
     "2023-03-01"
    )

    transformationTask1WithSimpleApproachWithComplicationsDF.show()

    // TASK 2

    // Simple approach.

    val transformationTask2WithSimpleApproachDF = transformationTask2WithSimpleApproach(
      notCompletedOrdersDF,
      completedOrdersDF,
      usersDF,
      productsDF,
      categoriesDF
    )

    transformationTask2WithSimpleApproachDF.show()

    // Approach with repartitioning.

    val transformationTask2WithRepartitioningApproachDF = transformationTask2WithRepartitioningApproach(
      notCompletedOrdersDF,
      completedOrdersDF,
      usersDF,
      productsDF,
      categoriesDF
    )

    transformationTask2WithRepartitioningApproachDF.show()

    // Simple approach with "complications".

    val transformationTask2WithSimpleApproachWithComplicationsDF = transformationTask2WithSimpleApproachWithComplications(
      notCompletedOrdersDF,
      completedOrdersDF,
      usersDF,
      productsDF,
      categoriesDF,
      "2023-03-01"
    )

    transformationTask2WithSimpleApproachWithComplicationsDF.show()

    spark.stop()
  }
}

