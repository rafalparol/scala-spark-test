import example.Task2SimpleApproach.transformationTask2WithSimpleApproach
import example.Task2SimpleApproachWithComplications.transformationTask2WithSimpleApproachWithComplications
import example.{TaskData, TaskInitialization}
import org.scalatest.flatspec.AnyFlatSpec

class Task2SimpleApproachWithComplicationsTest extends AnyFlatSpec {
  "Task2SimpleApproachWithComplications" should "returns correct DataFrame" in {
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

    // RUN

    val transformationTask2WithSimpleApproachWithComplicationsDF = transformationTask2WithSimpleApproachWithComplications(
      spark,
      notCompletedOrdersDF,
      completedOrdersDF,
      notCompletedPaymentsDF,
      completedPaymentsDF,
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

    val results = transformationTask2WithSimpleApproachWithComplicationsDF.collectAsList()

    assert(results.size() == 4)

    assert(results.get(0).getString(0) == "user-01")
    assert(results.get(1).getString(0) == "user-02")
    assert(results.get(2).getString(0) == "user-03")
    assert(results.get(3).getString(0) == "user-03")

    assert(results.get(0).getString(1) == "ANNE ANDERSON")
    assert(results.get(1).getString(1) == "TOMMY HARADA")
    assert(results.get(2).getString(1) == "STEPHANE MOREAU")
    assert(results.get(3).getString(1) == "STEPHANE MOREAU")

    assert(results.get(0).getString(2) == "office-laptops-01")
    assert(results.get(1).getString(2) == "premium-laptops-01")
    assert(results.get(2).getString(2) == "super-laptops-01")
    assert(results.get(3).getString(2) == "ultra-laptops-01")

    assert(results.get(0).getDouble(3) == 500.0)
    assert(results.get(1).getDouble(3) == 350.0)
    assert(results.get(2).getDouble(3) == 300.0)
    assert(results.get(3).getDouble(3) == 200.0)

    // CLEANUP

    // spark.stop()
  }
}