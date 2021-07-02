import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.TimestampType

object ContactBuilder {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  def getContact(df:DataFrame): DataFrame = {
    df.select($"sysAudit_sys_request_reference",$"agreement_number",$"agreement_component_id",$"coverage_id",explode($"contacts.contact").as("Contact"))
      .select($"sysAudit_sys_request_reference",$"agreement_number",$"coverage_id",$"agreement_component_id",$"Contact.*")
      .withColumnRenamed("contactId", "contact_id")
      .withColumnRenamed("crmId", "crm_id")
      .withColumnRenamed("contactType", "contact_type")
      .withColumnRenamed("dateOfBirth", "date_of_birth")
      .withColumn("date_of_birth", ($"date_of_birth").cast(TimestampType))
      .select($"agreement_number", $"agreement_component_id", $"coverage_id", $"contact_id", $"contact_type", $"crm_id", $"date_of_birth", $"sysAudit_sys_request_reference")
  }
}
