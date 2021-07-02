object DataArrays {
  val object_classArray = Array("io.ignatica.models.Coverage","io.ignatica.insurtech.model.Coverage","io.ignatica.insurance.models.Coverage","io.ignatica.insurance.models.Agreement","io.ignatica.insurance.models.AgreementComponent","io.ignatica.insurance.models.HoldingAccount")
  
  val failedColumns = Array("source_reference","object_class","account_name","issue","payload")
  
  val agreementColumns = Array("source_reference","agreement_holder","agreement_number","agreement_status","country","inception_date","issue_date","object_class","sysAudit_object_class"
    ,"sysAudit_sys_endpoint_method","sysAudit_sys_endpoint_uri","sysAudit_sys_endpoint_version","sysAudit_sys_tx_date_stamp","sysAudit_sys_request_reference","payload")
  
  val agreementComponentColumns = Array("source_reference",
    "object_class",
    "agreement_number",
    "agreement_component_id",
    "agreement_component_status",
    "country",
    "inception_date",
    "issue_date",
    "sysAudit_object_class",
    "sysAudit_sys_endpoint_method",
    "sysAudit_sys_endpoint_uri",
    "sysAudit_sys_endpoint_version",
    "sysAudit_sys_tx_date_stamp",
    "sysAudit_sys_request_reference",
    "payload")
  
  val CoverageColumns = Array("source_reference",
    "agreement_number",
    "agreement_component_id",
    "coverage_id",
    "asset",
    "auto_renew",
    "base_premium_amount",
    "base_premium_rate",
    "base_premium_rate_table_row_id",
    "billing_method",
    "billing_mode",
    "coverage_statuses",
    "effective_end_date",
    "face_amount",
    "final_premium_amount",
    "inception_date",
    "issue_date",
    "is_rider",
    "rider_ref",
    "no_claim_discount_applied",
    "no_claim_discount_rate",
    "tax_rate",
    "underwriting_parameters_json",
    "object_class",
    "sysAudit_object_class",
    "sysAudit_sys_endpoint_method",
    "sysAudit_sys_endpoint_uri",
    "sysAudit_sys_endpoint_version",
    "sysAudit_sys_tx_date_stamp",
    "sysAudit_sys_request_reference",
    "payload"
  )
  
  val HoldingAccountColumns = Array(
    "source_reference",
    "object_class",
    "agreement_component_id",
    "agreement_number",
    "coverage_id",
    "account_id",
    "account_name",
    "balance",
    "currency_code",
    "fund_id",
    "fund_price",
    "transaction_date",
    "transaction_type",
    "transaction_value",
    "wallet_address",
    "source_account",
    "sysAudit_object_class",
    "sysAudit_sys_endpoint_method",
    "sysAudit_sys_endpoint_uri",
    "sysAudit_sys_endpoint_version",
    "sysAudit_sys_tx_date_stamp",
    "sysAudit_sys_request_reference",
    "payload"
  )
  
  val HoldingAccountPaidColumns = Array(
    "source_reference",
    "object_class",
    "agreement_number",
    "agreement_component_id",
    "coverage_id",
    "account_id",
    "account_name",
    "balance",
    "transaction_date",
    "transaction_type",
    "wallet_address",
    "sysAudit_object_class",
    "sysAudit_sys_endpoint_method",
    "sysAudit_sys_endpoint_uri",
    "sysAudit_sys_endpoint_version",
    "sysAudit_sys_tx_date_stamp",
    "sysAudit_sys_request_reference",
    "payload"
  )
  
  val HoldingAccountFundColumns = Array(
    "source_reference",
    "object_class",
    "agreement_number",
    "agreement_component_id",
    "coverage_id",
    "account_id",
    "account_name",
    "balance",
    "currency_code",
    "fund_id",
    "fund_price",
    "transaction_date",
    "transaction_type",
    "transaction_value",
    "wallet_address",
    "source_account",
    "sysAudit_object_class",
    "sysAudit_sys_endpoint_method",
    "sysAudit_sys_endpoint_uri",
    "sysAudit_sys_endpoint_version",
    "sysAudit_sys_tx_date_stamp",
    "sysAudit_sys_request_reference",
    "payload",
    "pendingTransactions"
  )

  val CoverageDrop = Array("tax_rate","base_premium_rate_table_row_id","asset_object_class","no_claim_discount_applied","is_rider","rider_ref","no_claim_discount_rate")

  val HoldingAccountDrop = Array("pendingTransactions","status")
  val HoldingAccountDrop2 = Array("currency_code","fund_id","fund_price","coverage_id","agreement_component_id")


}
