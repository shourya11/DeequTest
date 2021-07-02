import org.apache.spark.sql.functions.{explode, to_json}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object CoverageBuilder {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  def applyPremiumModifier(df: DataFrame): DataFrame = {
    df.select($"agreement_number",$"agreement_component_id",$"coverage_id",explode($"basePremiumModifiers").as("BPM"),$"sysAudit_sys_request_reference")
      .select($"agreement_number", $"agreement_component_id", $"coverage_id", $"BPM.*", $"sysAudit_sys_request_reference")
      .withColumnRenamed("premiumModifierExcess", "excess")
      .withColumnRenamed("premiumModifierName", "name")
      .withColumnRenamed("premiumModifierPerc", "percentage")
      .withColumnRenamed("objectClass", "object_class")
  }

  def investmentOptions(df: DataFrame): DataFrame = {
    df.select($"agreement_number",$"agreement_component_id",$"coverage_id",$"investmentOptions.*",$"sysAudit_sys_request_reference")
      .withColumn("allocation",to_json($"allocation"))
      .withColumnRenamed("objectClass", "object_class")
  }

  def coverageDetails(df: DataFrame): DataFrame = {
    df.select($"agreement_number",$"agreement_component_id",$"coverage_id",explode($"coverages").as("c"),$"sysAudit_sys_request_reference")
      .select($"*",$"c.*",$"sysAudit_sys_request_reference".as("sys_r_r")).drop("c","sysAudit_sys_request_reference")
      .withColumnRenamed("sys_r_r","sysAudit_sys_request_reference")
      .withColumnRenamed("basePremiumRate","base_premium_rate")
      .withColumnRenamed("basePremiumRateTableRowId","base_premium_rate_table_row_id")
  }

  def changeCoverage(df: DataFrame): DataFrame = {
    df.withColumn("coverageStatuses", $"coverageStatuses".cast(StringType))
      .withColumn("underwriting_parameters", to_json($"underwriting_parameters"))
      .withColumnRenamed("coverageStatuses", "coverage_statuses")
//      .withColumn("asset", to_json($"asset"))
      .withColumnRenamed("basePremiumRateTableRowID", "base_premium_rate_table_row_id")
      .withColumnRenamed("underwriting_parameters", "underwriting_parameters_json")
//      .select($"*", $"asset.objectClass".as("asset_object_class")).drop("asset")
  }


  def buildCoverage(data: DataFrame) = {
    var basePremiumModifiers = applyPremiumModifier(data)
    var investment_options = investmentOptions(data)
    var coverage_detail = coverageDetails(data)
    var coverageContact = ContactBuilder.getContact(data)
    var coverage_df = changeCoverage(data)
    (basePremiumModifiers,investment_options, coverage_detail,coverageContact,coverage_df)
  }

}
