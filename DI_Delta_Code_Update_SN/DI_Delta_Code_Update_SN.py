# =============================================
# Author: Data Engineering Team - Regulatory Reporting
# Updated by: ASCENDION AVA+
# Created Date: 2025-04-23
# Updated on: 
# Description: Enhanced Spark Scala ETL for AML and compliance reporting. Adds REGION and LAST_AUDIT_DATE to BRANCH_SUMMARY_REPORT with conditional logic for active branches. Preserves legacy logic and ensures backward compatibility.
# =============================================

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

def main():
    # Initialize Spark session
    spark = SparkSession.builder().appName("RegulatoryReportingETL").getOrCreate()
    Logger.getLogger("org").setLevel(Level.WARN)
    logger = Logger.getLogger("RegulatoryReportingETL")

    # JDBC connection properties
    jdbcUrl = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
    connectionProps = {"user": "your_user", "password": "your_password"}

    def readTable(tableName):
        logger.info(f"Reading table: {tableName}")
        return spark.read.jdbc(jdbcUrl, tableName, connectionProps)

    # [ADDED] Read BRANCH_OPERATIONAL_DETAILS for new logic
    branchOpDF = readTable("BRANCH_OPERATIONAL_DETAILS")

    # Read other source tables
    customerDF = readTable("CUSTOMER")
    accountDF = readTable("ACCOUNT")
    transactionDF = readTable("TRANSACTION")
    branchDF = readTable("BRANCH")

    # Create AML_CUSTOMER_TRANSACTIONS DataFrame (unchanged)
    amlTransactionsDF = customerDF \
        .join(accountDF, "CUSTOMER_ID") \
        .join(transactionDF, "ACCOUNT_ID") \
        .select(
            col("CUSTOMER_ID"),
            col("NAME"),
            col("ACCOUNT_ID"),
            col("TRANSACTION_ID"),
            col("AMOUNT"),
            col("TRANSACTION_TYPE"),
            col("TRANSACTION_DATE")
        )
    amlTransactionsDF.write.format("delta").mode("overwrite").saveAsTable("AML_CUSTOMER_TRANSACTIONS")

    # [MODIFIED] Create BRANCH_SUMMARY_REPORT DataFrame with REGION and LAST_AUDIT_DATE
    # Join branch summary with operational details
    branchSummaryDF = transactionDF \
        .join(accountDF, "ACCOUNT_ID") \
        .join(branchDF, "BRANCH_ID") \
        .groupBy("BRANCH_ID", "BRANCH_NAME") \
        .agg(
            count("*").alias("TOTAL_TRANSACTIONS"),
            sum("AMOUNT").alias("TOTAL_AMOUNT")
        )

    # [ADDED] Join with operational details for REGION and LAST_AUDIT_DATE
    branchSummaryWithOpsDF = branchSummaryDF \
        .join(branchOpDF.select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE", "IS_ACTIVE"), "BRANCH_ID", "left") \
        .withColumn(
            "REGION",
            when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None))
        ) \
        .withColumn(
            "LAST_AUDIT_DATE",
            when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(lit(None))
        )
    # [MODIFIED] Select all required columns for target table
    branchSummaryFinalDF = branchSummaryWithOpsDF.select(
        col("BRANCH_ID"),
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT"),
        col("REGION"),
        col("LAST_AUDIT_DATE")
    )

    # [MODIFIED] Write to Delta table with new columns
    branchSummaryFinalDF.write.format("delta").mode("overwrite").saveAsTable("BRANCH_SUMMARY_REPORT")
    logger.info("Written to BRANCH_SUMMARY_REPORT with REGION and LAST_AUDIT_DATE")

    # [ADDED] Pre-load validation: Ensure all BRANCH_IDs in operational details exist in summary
    missingBranchIds = branchOpDF.join(branchSummaryDF, "BRANCH_ID", "left_anti")
    if missingBranchIds.count() > 0:
        logger.warn(f"Branches in operational details not present in summary: {missingBranchIds.count()}")

    # [ADDED] Post-load validation: Check REGION and LAST_AUDIT_DATE population
    invalidRegion = branchSummaryFinalDF.filter((col("IS_ACTIVE") == "Y") & (col("REGION").isNull() | col("LAST_AUDIT_DATE").isNull()))
    if invalidRegion.count() > 0:
        logger.warn(f"Active branches with missing REGION or LAST_AUDIT_DATE: {invalidRegion.count()}")

    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()

# =============================================
# [MODIFIED] - BRANCH_SUMMARY_REPORT now includes REGION and LAST_AUDIT_DATE for active branches only.
# [ADDED] - All changes are annotated inline as per requirements.
# =============================================
