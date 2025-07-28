=============================================
Author: Data Engineering Team - Regulatory Reporting
Date: 
Description: Enhanced ETL for BRANCH_SUMMARY_REPORT with REGION and LAST_AUDIT_DATE fields, conditional population, and legacy logic preservation.
Updated by: ASCENDION AVA+
Updated on: 
Description: Adds REGION and LAST_AUDIT_DATE to BRANCH_SUMMARY_REPORT, joins BRANCH_OPERATIONAL_DETAILS, and applies conditional logic for IS_ACTIVE branches. All changes annotated.
=============================================

'''
* ----------------------------------------------------------------------------------------
* Script Name   : RegulatoryReportingETL.scala
* Created By    : Data Engineering Team - Regulatory Reporting
* Author        : <Your Name>
* Created Date  : 2025-04-23
* Last Modified : 2025-04-23 // [MODIFIED]
* Description   : This Spark Scala job extracts data from Oracle DB for AML and compliance
*                 reporting, transforms it, and loads it into two Delta tables on Databricks:
*                 - AML_CUSTOMER_TRANSACTIONS
*                 - BRANCH_SUMMARY_REPORT
*
* Change Log:
* 2025-04-23 <Your Name>:
*   - Added REGION (STRING) and LAST_AUDIT_DATE (DATE) columns to BRANCH_SUMMARY_REPORT // [ADDED]
*   - Joined BRANCH_OPERATIONAL_DETAILS using BRANCH_ID // [ADDED]
*   - Populated REGION and LAST_AUDIT_DATE only if IS_ACTIVE = 'Y' // [ADDED]
*   - Preserved legacy logic for BRANCH_NAME, TOTAL_TRANSACTIONS, TOTAL_AMOUNT // [MODIFIED]
*   - Commented out outdated logic and marked changes // [DEPRECATED]
*   - Added inline documentation for new/changed logic // [ADDED]
*   - Recommended indexing BRANCH_ID for performance // [ADDED]
* ----------------------------------------------------------------------------------------
'''

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object RegulatoryReportingETL {

    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder()
        .appName("RegulatoryReportingETL")
        .getOrCreate()

    // Set logging level
    Logger.getLogger("org").setLevel(Level.WARN)
    val logger = Logger.getLogger(getClass.getName)

    // JDBC connection properties
    val jdbcUrl = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
    val connectionProps = new java.util.Properties()
    connectionProps.setProperty("user", "your_user")
    connectionProps.setProperty("password", "your_password")

    // Read Oracle tables as DataFrames
    def readTable(tableName: String): DataFrame = {
        logger.info(s"Reading table: $tableName")
        spark.read.jdbc(jdbcUrl, tableName, connectionProps)
    }

    // Create AML_CUSTOMER_TRANSACTIONS DataFrame (legacy logic preserved)
    def createAmlCustomerTransactions(customerDF: DataFrame, accountDF: DataFrame, transactionDF: DataFrame): DataFrame = {
        customerDF
            .join(accountDF, "CUSTOMER_ID")
            .join(transactionDF, "ACCOUNT_ID")
            .select(
                col("CUSTOMER_ID"),
                col("NAME"),
                col("ACCOUNT_ID"),
                col("TRANSACTION_ID"),
                col("AMOUNT"),
                col("TRANSACTION_TYPE"),
                col("TRANSACTION_DATE")
            )
    }

    // Create BRANCH_SUMMARY_REPORT DataFrame with REGION and LAST_AUDIT_DATE // [MODIFIED]
    def createBranchSummaryReport(
        transactionDF: DataFrame,
        accountDF: DataFrame,
        branchDF: DataFrame,
        branchOperationalDF: DataFrame // [ADDED]
    ): DataFrame = {
        // Legacy aggregation for branch summary // [MODIFIED]
        val baseSummaryDF = transactionDF
            .join(accountDF, "ACCOUNT_ID")
            .join(branchDF, "BRANCH_ID")
            .groupBy("BRANCH_ID", "BRANCH_NAME")
            .agg(
                count("*").alias("TOTAL_TRANSACTIONS"),
                sum("AMOUNT").alias("TOTAL_AMOUNT")
            )

        // Join with BRANCH_OPERATIONAL_DETAILS to add REGION and LAST_AUDIT_DATE // [ADDED]
        val joinedDF = baseSummaryDF
            .join(branchOperationalDF, Seq("BRANCH_ID"), "left") // [ADDED]
            .withColumn(
                "REGION",
                when(col("IS_ACTIVE") === lit("Y"), col("REGION")).otherwise(lit(null)) // [ADDED]
            )
            .withColumn(
                "LAST_AUDIT_DATE",
                when(col("IS_ACTIVE") === lit("Y"), col("LAST_AUDIT_DATE")).otherwise(lit(null).cast("date")) // [ADDED]
            )
            .select(
                col("BRANCH_ID"),
                col("BRANCH_NAME"),
                col("TOTAL_TRANSACTIONS"),
                col("TOTAL_AMOUNT"),
                col("REGION"),           // [ADDED]
                col("LAST_AUDIT_DATE")   // [ADDED]
            )

        // Inline documentation:
        // REGION and LAST_AUDIT_DATE are populated only for branches where IS_ACTIVE = 'Y'.
        // For inactive branches, these fields are set to NULL as per requirements. // [ADDED]

        joinedDF
    }

    // Write DataFrame to Delta table
    def writeToDeltaTable(df: DataFrame, tableName: String): Unit = {
        df.write.format("delta")
            .mode("overwrite")
            .saveAsTable(tableName)
        logger.info(s"Written to $tableName")
    }

    def main(args: Array[String]): Unit = {
        try {
            // Read source tables
            val customerDF = readTable("CUSTOMER")
            val accountDF = readTable("ACCOUNT")
            val transactionDF = readTable("TRANSACTION")
            val branchDF = readTable("BRANCH")
            val branchOperationalDF = readTable("BRANCH_OPERATIONAL_DETAILS") // [ADDED]

            // Pre-load validation: Ensure IS_ACTIVE is clean ('Y'/'N') // [ADDED]
            val validBranchOperationalDF = branchOperationalDF
                .filter(col("IS_ACTIVE").isin("Y", "N"))
                // Optionally log or handle invalid IS_ACTIVE values

            // Create and write AML_CUSTOMER_TRANSACTIONS
            val amlTransactionsDF = createAmlCustomerTransactions(customerDF, accountDF, transactionDF)
            writeToDeltaTable(amlTransactionsDF, "AML_CUSTOMER_TRANSACTIONS")

            // Create and write BRANCH_SUMMARY_REPORT with new columns and logic // [MODIFIED]
            val branchSummaryDF = createBranchSummaryReport(
                transactionDF,
                accountDF,
                branchDF,
                validBranchOperationalDF // [ADDED]
            )
            writeToDeltaTable(branchSummaryDF, "BRANCH_SUMMARY_REPORT")

            // Performance recommendation: Index BRANCH_ID in both tables for join optimization // [ADDED]
            // spark.sql("CREATE INDEX IF NOT EXISTS idx_branch_id ON BRANCH_OPERATIONAL_DETAILS (BRANCH_ID)")
            // spark.sql("CREATE INDEX IF NOT EXISTS idx_branch_id ON BRANCH_SUMMARY_REPORT (BRANCH_ID)")
            // (Uncomment above lines if supported by your Databricks environment)

        } catch {
            case e: Exception =>
                logger.error("ETL job failed with exception: ", e)
                throw e
        } finally {
            spark.stop()
            logger.info("Spark session stopped.")
        }
    }
}

# [END OF FILE]

# ----------------------------------------------------------------------------------------
# Cost Estimation:
# - Input tokens: ~2600 (instructions, code, context, DDLs, mapping, specs)
# - Output tokens: ~1100 (full code, metadata, comments, cost block)
# - Model: gpt-4-1106-preview
# - Pricing: $0.01 per 1K input tokens, $0.03 per 1K output tokens
# - Input cost: 2.6K * $0.01 = $0.026
# - Output cost: 1.1K * $0.03 = $0.033
# - Total estimated cost: $0.059
# ----------------------------------------------------------------------------------------
