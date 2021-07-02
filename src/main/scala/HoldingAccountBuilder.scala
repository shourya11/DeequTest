import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, to_json, unix_timestamp}
import org.apache.spark.sql.types.TimestampType

object HoldingAccountBuilder {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  def pendingTransaction(df:DataFrame): DataFrame = {
    var PendingTransaction =  df.select(
      $"sysAudit_sys_request_reference",
      $"agreement_number",
      $"agreement_component_id",
      $"coverage_id",
      $"account_id",
      $"account_name",
      $"transaction_type",
      $"fund_id",
      explode($"pendingTransactions").as("PT"))

    PendingTransaction = PendingTransaction.select($"*",$"PT.*").drop("PT")
    PendingTransaction = PendingTransaction.withColumn("date",($"date").cast(TimestampType))
      .withColumnRenamed("objectClass","object_class")
      .withColumnRenamed("sourceAccountId","source_account_id")
    PendingTransaction
  }

  def changeGeneric(df: DataFrame): DataFrame = {
    var HoldingAccountGeneric = df.withColumn("sourceAccount",to_json($"sourceAccount"))
      .withColumnRenamed("sourceAccount","source_account")
    HoldingAccountGeneric = HoldingAccountGeneric.drop(DataArrays.HoldingAccountDrop:_*)
    HoldingAccountGeneric = HoldingAccountGeneric.select(DataArrays.HoldingAccountColumns.map(m=>col(m)):_*)
    HoldingAccountGeneric

  }

  def changePaid(df: DataFrame): DataFrame = {
    val h = df.filter($"account_name" === "IG_ACT_PAIDTODATE")
    var HoldingAccount_Paid = h.drop("pendingTransactions","sourceAccount","status")
    HoldingAccount_Paid = HoldingAccount_Paid
      .withColumn("balance",unix_timestamp($"balance","yyyy-MM-dd'T'HH:mm:ss.SSSZ").cast(TimestampType))

    HoldingAccount_Paid = HoldingAccount_Paid.select(DataArrays.HoldingAccountPaidColumns.map(m=>col(m)):_*)
    HoldingAccount_Paid
  }

  def changeFund(df: DataFrame) = {
    val hf = df.filter($"account_name" === "IG_ACT_FUND")
    val PendingTransaction = pendingTransaction(hf)
    var HoldingAccount_Fund = hf.drop("status")
    HoldingAccount_Fund = HoldingAccount_Fund.withColumn("sourceAccount",to_json($"sourceAccount"))
      .withColumnRenamed("sourceAccount","source_account")

    HoldingAccount_Fund = HoldingAccount_Fund.select(DataArrays.HoldingAccountFundColumns.map(m=>col(m)):_*)
    (PendingTransaction,HoldingAccount_Fund)
  }


  def buildHoldingAccount(data: DataFrame) = {
    var holdingAccountGeneric = changeGeneric(data)
    var holdingAccountPaid = changePaid(data)
    var haf = changeFund(data)
    var pendingTransaction = haf._1
    var holdingAccountFund = haf._2
    (holdingAccountGeneric,holdingAccountPaid,pendingTransaction,holdingAccountFund)
  }

}
