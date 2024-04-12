import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUtils {
  def areDataFrameEquals(df1: DataFrame, df2: DataFrame): Boolean = {
    areDataFrameEqualsRegardingSchema(df1, df2) && areDataFrameEqualsRegardingData(df1, df2)
  }
  def areDataFrameEqualsRegardingSchema(df1: DataFrame, df2: DataFrame): Boolean = {
    val sch1 = df1.schema.fields.map(f => (f.name, f.dataType))
    val sch2 = df2.schema.fields.map(f => (f.name, f.dataType))
    sch1.diff(sch2).isEmpty && sch2.diff(sch1).isEmpty
  }
  def areDataFrameEqualsRegardingData(df1: DataFrame, df2: DataFrame): Boolean = {
    // Due to converting DFs to Arrays it will work only for small data sets and unit tests.
    val data1 = df1.collect()
    val data2 = df2.collect()
    data1.diff(data2).isEmpty && data2.diff(data1).isEmpty
  }

  def getBasicQueryExpectedResult(spark: SparkSession): DataFrame = {
    //  +----------------+--------------------+--------+
    //  |ConsideredUserId|ConsideredCategoryId|TotalSum|
    //  +----------------+--------------------+--------+
    //  |         user-01|   office-laptops-01|   600.0|
    //  |         user-02|  regular-laptops-01|   400.0|
    //  |         user-02|  premium-laptops-01|   350.0|
    //  |         user-03|    super-laptops-01|   300.0|
    //  |         user-03|    ultra-laptops-01|   200.0|
    //  +----------------+--------------------+--------+

    spark.createDataFrame(List(
        ("user-01", "office-laptops-01", 600.0),
        ("user-02", "regular-laptops-01", 400.0),
        ("user-02", "premium-laptops-01", 350.0),
        ("user-03", "super-laptops-01", 300.0),
        ("user-03", "ultra-laptops-01", 200.0)
      ))
      .withColumnRenamed("_1", "ConsideredUserId")
      .withColumnRenamed("_2", "ConsideredCategoryId")
      .withColumnRenamed("_3", "TotalSum")
  }

  def getComplicatedQueryExpectedResult(spark: SparkSession): DataFrame = {
    //  +----------------+-----------------------------+--------------------+--------+
    //  |ConsideredUserId|ConsideredFullNameInUpperCase|ConsideredCategoryId|TotalSum|
    //  +----------------+-----------------------------+--------------------+--------+
    //  |         user-01|                ANNE ANDERSON|   office-laptops-01|   500.0|
    //  |         user-02|                 TOMMY HARADA|  premium-laptops-01|   350.0|
    //  |         user-03|              STEPHANE MOREAU|    super-laptops-01|   300.0|
    //  |         user-03|              STEPHANE MOREAU|    ultra-laptops-01|   200.0|
    //  +----------------+-----------------------------+--------------------+--------+

    spark.createDataFrame(List(
        ("user-01", "ANNE ANDERSON", "office-laptops-01", 500.0),
        ("user-02", "TOMMY HARADA", "premium-laptops-01", 350.0),
        ("user-03", "STEPHANE MOREAU", "super-laptops-01", 300.0),
        ("user-03", "STEPHANE MOREAU", "ultra-laptops-01", 200.0)
      ))
      .withColumnRenamed("_1", "ConsideredUserId")
      .withColumnRenamed("_2", "ConsideredFullNameInUpperCase")
      .withColumnRenamed("_3", "ConsideredCategoryId")
      .withColumnRenamed("_4", "TotalSum")
  }
}
