import org.apache.spark.sql.functions.{array, col, expr, lit, to_json, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StringType

object FailedData {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._


  def failedObjectClass(df:DataFrame): DataFrame = {
    df.filter(!$"object_class".isin(DataArrays.object_classArray:_*) || $"object_class".isNull)
      .withColumn("issue",QueryData.inc2($"object_class"))
      .withColumn("account_name",$"payload.accountName")
      .select(DataArrays.failedColumns.map(m=>col(m)):_*)
      .withColumn("payload",to_json($"payload"))
  }

  def failedEntries(df:DataFrame): DataFrame = {
    var data_f = spark.emptyDataFrame
    val columnNameToCheck = "account_name"
    val dnull = df.columns.map(c => when(col(c).isNull || col(c) === "", lit(c)))
    var data_null = df.withColumn("issue", array(dnull: _*))
      .withColumn("issue", expr("array_join(issue, ',')"))

    if(!df.columns.contains(columnNameToCheck)) {
      data_null = data_null.withColumn("account_name", lit(null: String))
    }

    var haCondition = data_null.filter($"object_class" === "io.ignatica.insurance.models.HoldingAccount" && $"account_name" =!="IG_ACT_PAIDTODATE"  )
      .filter($"sysAudit_sys_endpoint_uri" =!= "/account")

    if (!haCondition.isEmpty){
      val fundCondition = haCondition
        .filter($"account_name" === "IG_ACT_FUND")
        .filter(($"fund_price".isNull || $"fund_price" === "") &&($"pendingTransactions".isNull))
      if (!fundCondition.isEmpty){
        haCondition = haCondition.filter($"account_name" =!= "IG_ACT_FUND")
        haCondition = haCondition.union(fundCondition)
      }
      else{
        haCondition = haCondition.filter($"account_name" =!= "IG_ACT_FUND")
      }
      data_f = haCondition.select(DataArrays.failedColumns.map(m=>col(m)):_*)
    }
    else {
      data_null = data_null.filter($"account_name".isNull || $"account_name" === "" || $"account_name" === "IG_ACT_PAIDTODATE")
      data_f = data_null.select(DataArrays.failedColumns.map(m=>col(m)):_*)
    }

    data_f = data_f.withColumn("account_name",$"account_name".cast(StringType))
      .filter($"issue".isNotNull && $"issue" =!= "")
    data_f
  }
}
