import example.Task2RepartitioningApproach.transformationTask2WithRepartitioningApproach
import example.{TaskData, TaskInitialization}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class Task2RepartitioningApproachTest extends AnyFlatSpec {
  "Task2RepartitioningApproach" should "returns correct DataFrame" in {
    // CREATE SPARK SESSION

    implicit val spark: SparkSession = TaskInitialization.createLocalSparkSession

    // CREATE TEST DATAFRAMES

    val usersDF = TaskData.createSampleUsersDF
    val productsDF = TaskData.createSampleProductsDF
    val categoriesDF = TaskData.createSampleCategoriesDF
    val completedOrdersDF = TaskData.createSampleCompletedOrdersDF
    val notCompletedOrdersDF = TaskData.createSampleNotCompletedOrdersDF

    // RUN

    val transformationTask2WithRepartitioningApproachDF = transformationTask2WithRepartitioningApproach(
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

    val obtainedResults = transformationTask2WithRepartitioningApproachDF
    val expectedResults = TestUtils.getBasicQueryExpectedResult(spark)

    assert(TestUtils.areDataFrameEquals(obtainedResults, expectedResults))

    // CLEANUP

    spark.stop()
  }
}
