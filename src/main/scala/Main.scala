import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.types.StringType

object Main {
  val spark : SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  def main() = {

    val ign = "C:\\Users\\shour\\Desktop\\Whiteklay\\DeequTest\\"
    println("reading data")

    val original_data = spark.read.schema(SchemaData.jsonSourceSchema).format("json").load("C:\\Users\\shour\\Desktop\\Whiteklay\\data\\*.json")

    val renamedData = RenameData.dataRenamed(original_data)

    val dataAnalyser = Deequ.analyser(renamedData)
    val dataVerification = Deequ.verification(renamedData)

    val data_f = FailedData.failedObjectClass(renamedData)

    println("processing agreement")

    var agreement = QueryData.dataFilter(renamedData,Array("io.ignatica.insurance.models.Agreement"))
    agreement = agreement.select(DataArrays.agreementColumns.map(m=>col(m)):_*)
    val verifyAgreement = Deequ.verificationColumns("agreement",agreement,DataArrays.agreementColumns.toSeq)
    val agreement_failed = FailedData.failedEntries(agreement)



    // Processing Agreement Component
    println("processing agreement Component")

    var agreementComponent = QueryData.dataFilter(renamedData,Array("io.ignatica.insurance.models.AgreementComponent"))
    val agreementContact = ContactBuilder.getContact(agreementComponent)

    agreementComponent = agreementComponent.select(DataArrays.agreementComponentColumns.map(m=>col(m)):_*)
    val verifyAgreementComponent = Deequ.verificationColumns("agreementComponent",agreementComponent,DataArrays.agreementComponentColumns.toSeq)
    val agreementComponent_failed = FailedData.failedEntries(agreementComponent)

    // Processing Coverage
    //
    println("processing coverages")

    val Coverages = QueryData.dataFilter(renamedData,Array("io.ignatica.insurtech.model.Coverage","io.ignatica.models.Coverage","io.ignatica.insurance.models.Coverage"))
    val (basePremiumModifiers,investment_options,coverage_detail,coverageContact,coverage_df) = CoverageBuilder.buildCoverage(Coverages)

    var mergeContact = spark.emptyDataFrame
    mergeContact = agreementContact.union(coverageContact)

    val cov = coverage_df.select(DataArrays.CoverageColumns.map(m=>col(m)):_*)
    val verifyCoverage = Deequ.verificationColumns("coverage",cov,cov.drop(DataArrays.CoverageDrop:_*).columns.toSeq)
    val Coverages_failed = FailedData.failedEntries(cov.drop(DataArrays.CoverageDrop:_*))


    // Processing Holding-Account
    //
    println("processing holding account")

    val HoldingAccount = QueryData.dataFilter(renamedData,Array("io.ignatica.insurance.models.HoldingAccount"))
    var (holdingAccountGeneric,holdingAccountPaid,pendingTransaction,holdingAccountFund) = HoldingAccountBuilder.buildHoldingAccount(HoldingAccount)

    // Processing Holding-Account-Generic
    //
    val verifyHoldingAccount = Deequ.verificationColumns("holdingAccount",
      holdingAccountGeneric.filter($"account_name" =!= "IG_ACT_FUND" && $"account_name" =!= "IG_ACT_PAIDTODATE"),
      holdingAccountGeneric.drop(DataArrays.HoldingAccountDrop2:_*).columns.toSeq)

    val HA_failed = FailedData.failedEntries(holdingAccountGeneric.drop(DataArrays.HoldingAccountDrop2:_*))
      .filter($"account_name" =!= "IG_ACT_FUND" && $"account_name" =!= "IG_ACT_PAIDTODATE")


    // Processing Holding-Account-Paid-To-Date
    //
    println("processing holding account paid")

    val verifyHoldingAccountPaid = Deequ.verificationColumns("holdingaccountpaid",
      holdingAccountPaid,
      holdingAccountPaid.drop("transaction_type").columns.toSeq)
    val HAP_failed = FailedData.failedEntries(holdingAccountPaid.drop("transaction_type"))

    // Processing Holding-Account-Fund
    //
    println("processing holding account fund")

    holdingAccountFund = holdingAccountFund.withColumn("pendingTransactions",$"pendingTransactions".cast(StringType))
    val verifyHoldingAccountFund = Deequ.verificationColumns("holdingaccountfund",
      holdingAccountFund,
      holdingAccountFund.drop("currency_code").columns.toSeq)

    val HAF_failed = FailedData.failedEntries(holdingAccountFund.drop("currency_code"))
    holdingAccountFund = holdingAccountFund.drop("pendingTransactions")

    // Deequ Verified
    //
    println("merging deequ verified dataframes")

    var dataVerified = dataVerification.union(verifyAgreement)
    val verifiedDfs = Array(verifyAgreementComponent,
      verifyCoverage,
      verifyHoldingAccount,
      verifyHoldingAccountPaid,
      verifyHoldingAccountFund)

    for (y <- verifiedDfs){
      dataVerified = dataVerified.union(y)
    }
    dataVerified = dataVerified.withColumn("t",current_timestamp())
    dataVerified = dataVerified.select($"t".as("system_timestamp"),$"*").drop("t")

//
    // Failed Data
    //
    var data_failed = data_f.union(agreement_failed)
    val failedDfs = Array(agreementComponent_failed,Coverages_failed,HA_failed,HAP_failed,HAF_failed)
    for(y <- failedDfs){
      data_failed = data_failed.union(y)
    }
    data_failed = data_failed.withColumn("t",current_timestamp())
    data_failed = data_failed.select($"t".as("system_timestamp"),$"*").drop("t")

    println("creating audit tables")

    var data_count = Seq(1).toDF("seq")
    data_count = data_count
      .withColumn("system_timestamp",current_timestamp())
      .withColumn("incoming_data_records",lit(renamedData.count()))
      .withColumn("agreement_records",lit(agreement.count()))
      .withColumn("agreement_component_records",lit(agreementComponent.count()))
      .withColumn("coverage_records",lit(cov.count()))
      .withColumn("holding_account_records",lit(holdingAccountGeneric.count()))
      .withColumn("holding_account_funds_records",lit(holdingAccountFund.count()))
      .withColumn("holding_account_paid_to_date_records",lit(holdingAccountPaid.count()))
      .withColumn("contact_records",lit(mergeContact.count()))
      .withColumn("pending_transaction_records",lit(pendingTransaction.count()))
      .withColumn("base_premium_modifier_records",lit(basePremiumModifiers.count()))
      .withColumn("investment_option_records",lit(investment_options.count()))
      .withColumn("coverage_details_records",lit(coverage_detail.count()))
      .drop("seq")


    val dfs = Array((agreement, ign + "agreement"),
      (agreementComponent , ign + "agreementComponent"),
      (mergeContact , ign + "contacts"),
      (cov , ign + "coverage"),
      (holdingAccountGeneric , ign + "holdingaccount"),
      (pendingTransaction , ign + "pendingtransaction"),
      (holdingAccountPaid , ign + "holdingaccountpaid"),
      (holdingAccountFund , ign + "holdingaccountfund"),
      (basePremiumModifiers , ign + "basePremiumModifier"),
      (investment_options , ign + "investmentOptions"),
      (coverage_detail , ign + "coverageDetails"),
      (data_count , ign + "audit"),
      (data_failed , ign + "auditFailed"),
      (dataVerified , ign + "auditVerified"),
      (dataAnalyser , ign + "auditAnalysed"),
    )

    dfs.foreach {
      case (df, format) => if (!df.isEmpty) {
        df.distinct().repartition(1).write.format("csv").option("header",value = true).mode("append").save(format)
      }
    }

    println("moving files")
    val jsonFiles = renamedData.select($"source_reference").collect().map(_.getString(0))
    QueryData.move_files(jsonFiles,"C:\\Users\\shour\\Desktop\\Whiteklay\\data\\","C:\\Users\\shour\\Desktop\\Whiteklay\\data_moved\\")


    val temp_data = spark.read.schema(SchemaData.jsonSourceSchema).format("json").load("C:\\Users\\shour\\Desktop\\Whiteklay\\data\\*.json")
    val renamedData2 = RenameData.dataRenamed(temp_data)
    val jsonFiles2 = renamedData2.select($"source_reference").collect().map(_.getString(0))
    QueryData.move_files(jsonFiles2,"C:\\Users\\shour\\Desktop\\Whiteklay\\temp_data\\","C:\\Users\\shour\\Desktop\\Whiteklay\\data\\")
    println("moved other files")

    println("ran successfully")


//  if (g.status != CheckStatus.Success) {

//    holdingAccountFund = holdingAccountFund.withColumn("issue", lit("Incorrect or missing Column"))
//  }
//  ////    .write.format("orc").mode("append").saveAsTable("bad_records")
//  ///}

  }
}


