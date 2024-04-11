import example.Task1SimpleApproach.transformationTask1WithSimpleApproach
import example.{TaskData, TaskInitialization}
import org.scalatest.flatspec.AnyFlatSpec

class Task1SimpleApproachTest extends AnyFlatSpec {
  "Task1SimpleApproach" should "returns correct DataFrame" in {
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

    val transformationTask1WithSimpleApproachDF = transformationTask1WithSimpleApproach(
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

    val results = transformationTask1WithSimpleApproachDF.collectAsList()

    assert(results.size() == 5)

    assert(results.get(0).getString(0) == "user-01")
    assert(results.get(1).getString(0) == "user-02")
    assert(results.get(2).getString(0) == "user-02")
    assert(results.get(3).getString(0) == "user-03")
    assert(results.get(4).getString(0) == "user-03")

    assert(results.get(0).getString(1) == "office-laptops-01")
    assert(results.get(1).getString(1) == "regular-laptops-01")
    assert(results.get(2).getString(1) == "premium-laptops-01")
    assert(results.get(3).getString(1) == "super-laptops-01")
    assert(results.get(4).getString(1) == "ultra-laptops-01")

    assert(results.get(0).getDouble(2) == 600.0)
    assert(results.get(1).getDouble(2) == 400.0)
    assert(results.get(2).getDouble(2) == 350.0)
    assert(results.get(3).getDouble(2) == 300.0)
    assert(results.get(4).getDouble(2) == 200.0)

    // CLEANUP

    spark.stop()
  }
}
