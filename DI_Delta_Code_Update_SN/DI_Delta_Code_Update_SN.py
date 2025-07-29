=============================================
Author: Data Engineering Team - Regulatory Reporting
Updated by: ASCENDION AVA+
Date: 
Updated on: 
Description: Enhanced ETL logic to support conditional population of REGION and LAST_AUDIT_DATE in BRANCH_SUMMARY_REPORT based on IS_ACTIVE flag, preserving legacy mappings and ensuring schema evolution traceability.
=============================================

'''
This script updates the ETL process for BRANCH_SUMMARY_REPORT to:
- Add REGION and LAST_AUDIT_DATE fields (populated only for active branches)
- Preserve legacy logic for BRANCH_NAME, TOTAL_TRANSACTIONS, and TOTAL_AMOUNT
- Comment out deprecated or changed logic
- Annotate all changes for traceability
'''

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

def RegulatoryReportingETL():
    # Initialize Spark session
    spark = SparkSession.builder().appName("RegulatoryReportingETL").getOrCreate()
    Logger.getLogger("org").setLevel(Level.WARN)
    logger = Logger.getLogger("RegulatoryReportingETL")

    jdbcUrl = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
    connectionProps = {"user": "your_user", "password": "your_password"}

    def readTable(tableName):
        logger.info(f"Reading table: {tableName}")
        return spark.read.jdbc(jdbcUrl, tableName, connectionProps)

    # [ADDED] Read branch operational details for REGION and LAST_AUDIT_DATE
    branchOpDF = readTable("BRANCH_OPERATIONAL_DETAILS")  # [ADDED]

    def createBranchSummaryReport(transactionDF, accountDF, branchDF, branchOpDF):
        # [MODIFIED] Join with branch operational details
        joinedDF = (
            transactionDF
            .join(accountDF, "ACCOUNT_ID")
            .join(branchDF, "BRANCH_ID")
            .join(branchOpDF, "BRANCH_ID", "left")  # [ADDED]
        )
        # [MODIFIED] Apply conditional logic for REGION and LAST_AUDIT_DATE
        resultDF = (
            joinedDF
            .groupBy("BRANCH_ID", "BRANCH_NAME")
            .agg(
                count("*").alias("TOTAL_TRANSACTIONS"),
                sum("AMOUNT").alias("TOTAL_AMOUNT"),
                first(when(col("IS_ACTIVE") == 'Y', col("REGION"))).alias("REGION"),  # [ADDED]
                first(when(col("IS_ACTIVE") == 'Y', col("LAST_AUDIT_DATE"))).alias("LAST_AUDIT_DATE")  # [ADDED]
            )
        )
        return resultDF

    def writeToDeltaTable(df, tableName):
        df.write.format("delta").mode("overwrite").saveAsTable(tableName)
        logger.info(f"Written to {tableName}")

    try:
        customerDF = readTable("CUSTOMER")
        accountDF = readTable("ACCOUNT")
        transactionDF = readTable("TRANSACTION")
        branchDF = readTable("BRANCH")
        # branchOpDF already read above

        # [UNCHANGED] Create and write AML_CUSTOMER_TRANSACTIONS
        amlTransactionsDF = customerDF.join(accountDF, "CUSTOMER_ID").join(transactionDF, "ACCOUNT_ID").select(
            col("CUSTOMER_ID"), col("NAME"), col("ACCOUNT_ID"), col("TRANSACTION_ID"), col("AMOUNT"), col("TRANSACTION_TYPE"), col("TRANSACTION_DATE")
        )
        writeToDeltaTable(amlTransactionsDF, "AML_CUSTOMER_TRANSACTIONS")

        # [MODIFIED] Create and write BRANCH_SUMMARY_REPORT with new fields
        branchSummaryDF = createBranchSummaryReport(transactionDF, accountDF, branchDF, branchOpDF)  # [MODIFIED]
        writeToDeltaTable(branchSummaryDF, "BRANCH_SUMMARY_REPORT")

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

# [NOTE] All changes are annotated with [ADDED], [MODIFIED], or [DEPRECATED] for traceability.
# [NOTE] REGION and LAST_AUDIT_DATE are only populated for active branches (IS_ACTIVE = 'Y').
# [NOTE] Legacy logic for BRANCH_NAME, TOTAL_TRANSACTIONS, and TOTAL_AMOUNT is preserved.
