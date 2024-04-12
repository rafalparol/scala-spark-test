import example.Task1RepartitioningApproach.transformationTask1WithRepartitioningApproach
import example.{TaskData, TaskInitialization}
import org.scalatest.flatspec.AnyFlatSpec

class Task1RepartitioningApproachTest extends AnyFlatSpec {
  "Task1RepartitioningApproach" should "returns correct DataFrame" in {
    // CREATE SPARK SESSION

    val spark = TaskInitialization.createLocalSparkSession

    // CREATE TEST DATAFRAMES

    val usersDF = TaskData.createSampleUsersDF(spark)
    val productsDF = TaskData.createSampleProductsDF(spark)
    val categoriesDF = TaskData.createSampleCategoriesDF(spark)
    val completedOrdersDF = TaskData.createSampleCompletedOrdersDF(spark)
    val notCompletedOrdersDF = TaskData.createSampleNotCompletedOrdersDF(spark)

    // RUN

    val transformationTask1WithRepartitioningApproachDF = transformationTask1WithRepartitioningApproach(
      spark,
      notCompletedOrdersDF,
      completedOrdersDF,
      usersDF,
      productsDF,
      categoriesDF
    )

    // CHECK

    //  +----------------+--------------------+--------+
    //  |ConsideredUserId|ConsideredCategoryId|TotalSum|
    //  +----------------+--------------------+--------+
    //  |         user-01|   office-laptops-01|   600.0|
    //  |         user-02|  regular-laptops-01|   400.0|
    //  |         user-02|  premium-laptops-01|   350.0|
    //  |         user-03|    super-laptops-01|   300.0|
    //  |         user-03|    ultra-laptops-01|   200.0|
    //  +----------------+--------------------+--------+

    val obtainedResults = transformationTask1WithRepartitioningApproachDF
    val expectedResults = TestUtils.getBasicQueryExpectedResult(spark)

    assert(TestUtils.areDataFrameEquals(obtainedResults, expectedResults))

    // CLEANUP

    spark.stop()
  }
}
