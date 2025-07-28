=============================================
Author: Data Engineering Team - Regulatory Reporting
Date: 
Description: Enhanced ETL to support schema evolution for BRANCH_SUMMARY_REPORT, including REGION and LAST_AUDIT_DATE fields, with conditional logic for active branches and legacy logic preservation.
Updated by: ASCENDION AVA+
Updated on: 
Description: This output updates the Scala ETL logic to join BRANCH_OPERATIONAL_DETAILS, conditionally populate REGION and LAST_AUDIT_DATE for active branches, and annotate all changes for traceability. Legacy logic is preserved and all modifications are clearly marked.
=============================================

# NOTE: This is a Scala-to-Python-style pseudocode conversion for illustration, as per .py requirement. Replace DataFrame API usage with Scala equivalents if needed.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when
import logging

class RegulatoryReportingETL:
    def __init__(self):
        self.spark = SparkSession.builder.appName("RegulatoryReportingETL").getOrCreate()
        logging.basicConfig(level=logging.WARN)
        self.logger = logging.getLogger("RegulatoryReportingETL")
        self.jdbcUrl = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
        self.connectionProps = {"user": "your_user", "password": "your_password"}

    def read_table(self, table_name):
        self.logger.info(f"Reading table: {table_name}")
        return self.spark.read.jdbc(self.jdbcUrl, table_name, properties=self.connectionProps)

    def create_aml_customer_transactions(self, customer_df, account_df, transaction_df):
        return (customer_df
                .join(account_df, "CUSTOMER_ID")
                .join(transaction_df, "ACCOUNT_ID")
                .select("CUSTOMER_ID", "NAME", "ACCOUNT_ID", "TRANSACTION_ID", "AMOUNT", "TRANSACTION_TYPE", "TRANSACTION_DATE"))

    def create_branch_summary_report(self, transaction_df, account_df, branch_df, branch_operational_df):
        # [MODIFIED] Join with BRANCH_OPERATIONAL_DETAILS for REGION and LAST_AUDIT_DATE
        joined_df = (transaction_df
                     .join(account_df, "ACCOUNT_ID")
                     .join(branch_df, "BRANCH_ID")
                     .join(branch_operational_df, ["BRANCH_ID"], "left"))

        # [ADDED] Conditional logic for REGION and LAST_AUDIT_DATE only for active branches
        result_df = (joined_df
                     .groupBy("BRANCH_ID", "BRANCH_NAME")
                     .agg(count("*").alias("TOTAL_TRANSACTIONS"),
                          sum("AMOUNT").alias("TOTAL_AMOUNT"),
                          # [ADDED] Populate REGION only if IS_ACTIVE = 'Y'
                          when(col("IS_ACTIVE") == 'Y', col("REGION")).alias("REGION"),
                          # [ADDED] Populate LAST_AUDIT_DATE only if IS_ACTIVE = 'Y'
                          when(col("IS_ACTIVE") == 'Y', col("LAST_AUDIT_DATE")).alias("LAST_AUDIT_DATE")))
        # [MODIFIED] REGION and LAST_AUDIT_DATE are nullable for backward compatibility
        return result_df

    def write_to_delta_table(self, df, table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        self.logger.info(f"Written to {table_name}")

    def validate_branch_ids(self, branch_operational_df, branch_summary_df):
        # [ADDED] Pre-load check: Ensure all BRANCH_IDs in BRANCH_OPERATIONAL_DETAILS exist in BRANCH_SUMMARY_REPORT
        missing_branches = branch_operational_df.join(branch_summary_df, ["BRANCH_ID"], "left_anti")
        count_missing = missing_branches.count()
        if count_missing > 0:
            self.logger.warn(f"{count_missing} BRANCH_ID(s) in BRANCH_OPERATIONAL_DETAILS missing in BRANCH_SUMMARY_REPORT")

    def validate_is_active(self, branch_operational_df):
        # [ADDED] Pre-load check: Ensure IS_ACTIVE values are only 'Y' or 'N'
        invalid = branch_operational_df.filter(~col("IS_ACTIVE").isin(['Y', 'N']))
        count_invalid = invalid.count()
        if count_invalid > 0:
            self.logger.warn(f"{count_invalid} invalid IS_ACTIVE values found")

    def main(self):
        try:
            customer_df = self.read_table("CUSTOMER")
            account_df = self.read_table("ACCOUNT")
            transaction_df = self.read_table("TRANSACTION")
            branch_df = self.read_table("BRANCH")
            branch_operational_df = self.read_table("BRANCH_OPERATIONAL_DETAILS")  # [ADDED]

            aml_transactions_df = self.create_aml_customer_transactions(customer_df, account_df, transaction_df)
            self.write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

            # [ADDED] Pre-load validation
            branch_summary_df = self.create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
            self.validate_branch_ids(branch_operational_df, branch_summary_df)
            self.validate_is_active(branch_operational_df)

            self.write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

            # [ADDED] Post-load checks (pseudo-code, implement as needed)
            # - Verify REGION and LAST_AUDIT_DATE for active branches
            # - Reconcile record counts

        except Exception as e:
            self.logger.error(f"ETL job failed with exception: {e}")
            raise
        finally:
            self.spark.stop()
            self.logger.info("Spark session stopped.")

# [DEPRECATED] Old createBranchSummaryReport logic (Scala style)
# def createBranchSummaryReport(transactionDF: DataFrame, accountDF: DataFrame, branchDF: DataFrame): DataFrame = {
#     transactionDF
#         .join(accountDF, "ACCOUNT_ID")
#         .join(branchDF, "BRANCH_ID")
#         .groupBy("BRANCH_ID", "BRANCH_NAME")
#         .agg(
#             count("*").alias("TOTAL_TRANSACTIONS"),
#             sum("AMOUNT").alias("TOTAL_AMOUNT")
#         )
# }
# [DEPRECATED] Above block is preserved for traceability. Use new logic with BRANCH_OPERATIONAL_DETAILS.

if __name__ == "__main__":
    etl = RegulatoryReportingETL()
    etl.main()

"""
Cost Estimation and Justification
---------------------------------
- Input tokens: <input_token_count>
- Output tokens: <output_token_count>
- Model used: <model_name>
- Pricing:
    Input cost per token: <input_cost_per_token>
    Output cost per token: <output_cost_per_token>
- Input Cost = input_tokens * input_cost_per_token
- Output Cost = output_tokens * output_cost_per_token
- Total Cost = Input Cost + Output Cost
"""
