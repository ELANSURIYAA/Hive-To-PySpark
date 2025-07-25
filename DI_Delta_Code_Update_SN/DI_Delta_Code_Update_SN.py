# =============================================
# Author: Data Engineering Team - Regulatory Reporting
# Updated by: ASCENDION AVA+
# Updated on: 
# Description: Enhanced Spark Scala ETL for AML and compliance reporting. Adds conditional REGION and LAST_AUDIT_DATE fields to BRANCH_SUMMARY_REPORT, joins with BRANCH_OPERATIONAL_DETAILS, and preserves legacy logic with clear annotations.
# =============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, when
import logging

# Initialize Spark session
spark = SparkSession.builder.appName("RegulatoryReportingETL").getOrCreate()

# Set logging level
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger("RegulatoryReportingETL")

# JDBC connection properties
jdbcUrl = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
connectionProps = {"user": "your_user", "password": "your_password"}

# Read Oracle tables as DataFrames
def readTable(tableName):
    logger.info(f"Reading table: {tableName}")
    return spark.read.jdbc(jdbcUrl, tableName, properties=connectionProps)

# [ADDED] Read BRANCH_OPERATIONAL_DETAILS table
def readBranchOperationalDetails():
    return readTable("BRANCH_OPERATIONAL_DETAILS")

# [MODIFIED] Create BRANCH_SUMMARY_REPORT DataFrame with new fields and logic
def createBranchSummaryReport(transactionDF, accountDF, branchDF, branchOpDF):
    # Legacy summary
    summaryDF = (
        transactionDF
        .join(accountDF, "ACCOUNT_ID")
        .join(branchDF, "BRANCH_ID")
        .groupBy("BRANCH_ID", "BRANCH_NAME")
        .agg(
            count("*").alias("TOTAL_TRANSACTIONS"),
            _sum("AMOUNT").alias("TOTAL_AMOUNT")
        )
    )
    # [ADDED] Join with BRANCH_OPERATIONAL_DETAILS for REGION and LAST_AUDIT_DATE
    joinedDF = summaryDF.join(branchOpDF, "BRANCH_ID", "left")
    # [ADDED] Conditional population for REGION and LAST_AUDIT_DATE
    resultDF = (
        joinedDF
        .withColumn(
            "REGION",
            when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None) # [ADDED]
        )
        .withColumn(
            "LAST_AUDIT_DATE",
            when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None) # [ADDED]
        )
    )
    return resultDF.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )

# Write DataFrame to Delta table
def writeToDeltaTable(df, tableName):
    df.write.format("delta").mode("overwrite").saveAsTable(tableName)
    logger.info(f"Written to {tableName}")

# [ADDED] Pre-load validation checks
def validateBranchIds(branchOpDF, summaryDF):
    missing = branchOpDF.join(summaryDF, "BRANCH_ID", "left_anti")
    count_missing = missing.count()
    if count_missing > 0:
        logger.warn(f"{count_missing} BRANCH_IDs in BRANCH_OPERATIONAL_DETAILS not found in BRANCH_SUMMARY_REPORT")

# [ADDED] IS_ACTIVE value normalization
def normalizeIsActive(branchOpDF):
    return branchOpDF.withColumn("IS_ACTIVE", when(col("IS_ACTIVE") == "Y", "Y").otherwise("N"))

# [ADDED] Post-load validation checks
def postLoadValidation(resultDF):
    activeBranches = resultDF.filter(col("REGION").isNotNull() & col("LAST_AUDIT_DATE").isNotNull())
    logger.info(f"Active branches with REGION and LAST_AUDIT_DATE populated: {activeBranches.count()}")

# Main ETL execution
def main():
    try:
        customerDF = readTable("CUSTOMER")
        accountDF = readTable("ACCOUNT")
        transactionDF = readTable("TRANSACTION")
        branchDF = readTable("BRANCH")
        branchOpDF = readBranchOperationalDetails() # [ADDED]

        # [ADDED] Normalize IS_ACTIVE values
        branchOpDF = normalizeIsActive(branchOpDF)

        # [ADDED] Pre-load validation
        summaryDF = (
            transactionDF
            .join(accountDF, "ACCOUNT_ID")
            .join(branchDF, "BRANCH_ID")
            .groupBy("BRANCH_ID", "BRANCH_NAME")
            .agg(count("*").alias("TOTAL_TRANSACTIONS"), _sum("AMOUNT").alias("TOTAL_AMOUNT"))
        )
        validateBranchIds(branchOpDF, summaryDF)

        # Create and write AML_CUSTOMER_TRANSACTIONS (legacy logic preserved)
        amlTransactionsDF = (
            customerDF
            .join(accountDF, "CUSTOMER_ID")
            .join(transactionDF, "ACCOUNT_ID")
            .select(
                "CUSTOMER_ID", "NAME", "ACCOUNT_ID", "TRANSACTION_ID", "AMOUNT", "TRANSACTION_TYPE", "TRANSACTION_DATE"
            )
        )
        writeToDeltaTable(amlTransactionsDF, "AML_CUSTOMER_TRANSACTIONS")

        # [MODIFIED] Create and write BRANCH_SUMMARY_REPORT with new logic
        branchSummaryDF = createBranchSummaryReport(transactionDF, accountDF, branchDF, branchOpDF)
        writeToDeltaTable(branchSummaryDF, "BRANCH_SUMMARY_REPORT")

        # [ADDED] Post-load validation
        postLoadValidation(branchSummaryDF)

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
