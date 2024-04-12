import example.Task2SimpleApproachWithComplications.transformationTask2WithSimpleApproachWithComplications
import example.{TaskData, TaskInitialization}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class Task2SimpleApproachWithComplicationsTest extends AnyFlatSpec {
  "Task2SimpleApproachWithComplications" should "returns correct DataFrame" in {
    // CREATE SPARK SESSION

    implicit val spark: SparkSession = TaskInitialization.createLocalSparkSession

    // CREATE TEST DATAFRAMES

    val usersDF = TaskData.createSampleUsersDF
    val productsDF = TaskData.createSampleProductsDF
    val categoriesDF = TaskData.createSampleCategoriesDF
    val completedOrdersDF = TaskData.createSampleCompletedOrdersDF
    val notCompletedOrdersDF = TaskData.createSampleNotCompletedOrdersDF

    // RUN

    val transformationTask2WithSimpleApproachWithComplicationsDF = transformationTask2WithSimpleApproachWithComplications(
      notCompletedOrdersDF,
      completedOrdersDF,
      usersDF,
      productsDF,
      categoriesDF,
      "2023-03-01"
    )

    // CHECK

    //  +----------------+-----------------------------+--------------------+--------+
    //  |ConsideredUserId|ConsideredFullNameInUpperCase|ConsideredCategoryId|TotalSum|
    //  +----------------+-----------------------------+--------------------+--------+
    //  |         user-01|                ANNE ANDERSON|   office-laptops-01|   500.0|
    //  |         user-02|                 TOMMY HARADA|  premium-laptops-01|   350.0|
    //  |         user-03|              STEPHANE MOREAU|    super-laptops-01|   300.0|
    //  |         user-03|              STEPHANE MOREAU|    ultra-laptops-01|   200.0|
    //  +----------------+-----------------------------+--------------------+--------+

    val obtainedResults = transformationTask2WithSimpleApproachWithComplicationsDF
    val expectedResults = TestUtils.getComplicatedQueryExpectedResult(spark)

    assert(TestUtils.areDataFrameEquals(obtainedResults, expectedResults))

    // CLEANUP

    spark.stop()
  }
}
