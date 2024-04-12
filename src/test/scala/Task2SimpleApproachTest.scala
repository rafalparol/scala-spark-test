import example.Task2SimpleApproach.transformationTask2WithSimpleApproach
import example.{TaskDatabase, TaskInitialization}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class Task2SimpleApproachTest extends AnyFlatSpec {
  "Task2SimpleApproach" should "returns correct DataFrame" in {
    // CREATE SPARK SESSION

    implicit val spark: SparkSession = TaskInitialization.createLocalSparkSession

    // CREATE TEST DATAFRAMES

    val usersDF = TaskDatabase.loadSampleUsersDF
    val productsDF = TaskDatabase.loadSampleProductsDF
    val categoriesDF = TaskDatabase.loadSampleCategoriesDF
    val completedOrdersDF = TaskDatabase.loadSampleCompletedOrdersDF
    val notCompletedOrdersDF = TaskDatabase.loadSampleNotCompletedOrdersDF

    // RUN

    val transformationTask2WithSimpleApproachDF = transformationTask2WithSimpleApproach(
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

    val obtainedResults = transformationTask2WithSimpleApproachDF
    val expectedResults = TestUtils.getBasicQueryExpectedResult(spark)

    assert(TestUtils.areDataFrameEquals(obtainedResults, expectedResults))

    // CLEANUP

    spark.stop()
  }
}
