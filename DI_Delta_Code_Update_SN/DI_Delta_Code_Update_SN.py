# =============================================
# Author: Data Engineering Team - Regulatory Reporting
# Date: 
# Description: Enhanced Spark Scala ETL for regulatory reporting with conditional population of REGION and LAST_AUDIT_DATE in BRANCH_SUMMARY_REPORT based on IS_ACTIVE status, preserving legacy logic and ensuring schema evolution traceability.
# Updated by: ASCENDION AVA+
# Updated on: 
# Description: This output enhances the ETL pipeline to join BRANCH_OPERATIONAL_DETAILS, conditionally populate REGION and LAST_AUDIT_DATE for active branches, and annotate all changes for compliance and maintainability.
# =============================================

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

    // Create AML_CUSTOMER_TRANSACTIONS DataFrame
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

    // [DEPRECATED] Previous BRANCH_SUMMARY_REPORT logic (without REGION and LAST_AUDIT_DATE)
    // def createBranchSummaryReport(transactionDF: DataFrame, accountDF: DataFrame, branchDF: DataFrame): DataFrame = {
    //     transactionDF
    //         .join(accountDF, "ACCOUNT_ID")
    //         .join(branchDF, "BRANCH_ID")
    //         .groupBy("BRANCH_ID", "BRANCH_NAME")
    //         .agg(
    //             count("*").alias("TOTAL_TRANSACTIONS"),
    //             sum("AMOUNT").alias("TOTAL_AMOUNT")
    //         )
    // }

    /**
     * [MODIFIED] Enhanced BRANCH_SUMMARY_REPORT logic to join BRANCH_OPERATIONAL_DETAILS and conditionally populate REGION and LAST_AUDIT_DATE.
     * - REGION and LAST_AUDIT_DATE are populated only for active branches (IS_ACTIVE = 'Y').
     * - Legacy logic for BRANCH_NAME, TOTAL_TRANSACTIONS, and TOTAL_AMOUNT is preserved.
     * - All changes are annotated for traceability.
     */
    def createBranchSummaryReport(
        transactionDF: DataFrame,
        accountDF: DataFrame,
        branchDF: DataFrame,
        branchOpDF: DataFrame
    ): DataFrame = {
        val joinedDF = transactionDF
            .join(accountDF, "ACCOUNT_ID")
            .join(branchDF, "BRANCH_ID")
            .join(branchOpDF, Seq("BRANCH_ID"), "left") // [ADDED] Join with operational details

        joinedDF
            .withColumn("REGION", when(col("IS_ACTIVE") === lit("Y"), col("REGION")).otherwise(lit(null))) // [ADDED] Conditional REGION
            .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === lit("Y"), col("LAST_AUDIT_DATE")).otherwise(lit(null))) // [ADDED] Conditional LAST_AUDIT_DATE
            .groupBy("BRANCH_ID", "BRANCH_NAME", "REGION", "LAST_AUDIT_DATE")
            .agg(
                count("*").alias("TOTAL_TRANSACTIONS"),
                sum("AMOUNT").alias("TOTAL_AMOUNT")
            )
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
            val branchOpDF = readTable("BRANCH_OPERATIONAL_DETAILS") // [ADDED] Read operational details

            // Create and write AML_CUSTOMER_TRANSACTIONS
            val amlTransactionsDF = createAmlCustomerTransactions(customerDF, accountDF, transactionDF)
            writeToDeltaTable(amlTransactionsDF, "AML_CUSTOMER_TRANSACTIONS")

            // Create and write BRANCH_SUMMARY_REPORT with enhanced logic
            val branchSummaryDF = createBranchSummaryReport(transactionDF, accountDF, branchDF, branchOpDF) // [MODIFIED]
            writeToDeltaTable(branchSummaryDF, "BRANCH_SUMMARY_REPORT")

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

// [ADDED] Validation and performance recommendations:
// - Ensure BRANCH_ID is indexed in both BRANCH_OPERATIONAL_DETAILS and BRANCH_SUMMARY_REPORT for join performance.
// - Filter out inactive branches (IS_ACTIVE = 'N') early if possible.
// - Pre-load and post-load checks to validate data integrity as per technical specifications.
