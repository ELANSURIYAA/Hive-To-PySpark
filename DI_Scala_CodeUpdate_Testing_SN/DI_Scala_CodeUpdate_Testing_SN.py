=============================================
Author: Data Engineering Team - Regulatory Reporting
Date: 2025-04-23
Description: This Spark Scala job extracts data from Oracle DB for AML and compliance reporting, transforms it, and loads it into two Delta tables on Databricks:
- AML_CUSTOMER_TRANSACTIONS
- BRANCH_SUMMARY_REPORT
Updated by: ASCENDION AVA+
Updated on: 
Description: Enhanced ETL to join BRANCH_SUMMARY_REPORT with BRANCH_OPERATIONAL_DETAILS, add REGION and LAST_AUDIT_DATE for active branches, and annotate all changes for traceability.
=============================================

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

    // [MODIFIED] Create BRANCH_SUMMARY_REPORT DataFrame with REGION and LAST_AUDIT_DATE
    /**
     * [MODIFIED]
     * Joins BRANCH_SUMMARY_REPORT with BRANCH_OPERATIONAL_DETAILS to add REGION and LAST_AUDIT_DATE
     * Only populates REGION and LAST_AUDIT_DATE for active branches (IS_ACTIVE = 'Y')
     * Preserves legacy logic for other fields
     */
    def createBranchSummaryReport(
        transactionDF: DataFrame,
        accountDF: DataFrame,
        branchDF: DataFrame,
        branchOperationalDF: DataFrame // [ADDED] New DataFrame for operational details
    ): DataFrame = {
        val baseSummaryDF = transactionDF
            .join(accountDF, "ACCOUNT_ID")
            .join(branchDF, "BRANCH_ID")
            .groupBy("BRANCH_ID", "BRANCH_NAME")
            .agg(
                count("*").alias("TOTAL_TRANSACTIONS"),
                sum("AMOUNT").alias("TOTAL_AMOUNT")
            )

        // [ADDED] Join with BRANCH_OPERATIONAL_DETAILS and apply conditional logic
        val joinedDF = baseSummaryDF
            .join(branchOperationalDF, Seq("BRANCH_ID"), "left") // [ADDED] Join for enrichment
            .withColumn(
                "REGION",
                when(col("IS_ACTIVE") === lit("Y"), col("REGION")).otherwise(lit(null)) // [ADDED] Conditional REGION
            )
            .withColumn(
                "LAST_AUDIT_DATE",
                when(col("IS_ACTIVE") === lit("Y"), col("LAST_AUDIT_DATE")).otherwise(lit(null)) // [ADDED] Conditional LAST_AUDIT_DATE
            )
            // [MODIFIED] Select columns in new order for target schema
            .select(
                col("BRANCH_ID"),
                col("BRANCH_NAME"),
                col("TOTAL_TRANSACTIONS"),
                col("TOTAL_AMOUNT"),
                col("REGION"),
                col("LAST_AUDIT_DATE")
            )
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

            // Create and write AML_CUSTOMER_TRANSACTIONS
            val amlTransactionsDF = createAmlCustomerTransactions(customerDF, accountDF, transactionDF)
            writeToDeltaTable(amlTransactionsDF, "AML_CUSTOMER_TRANSACTIONS")

            // [DEPRECATED] Old logic for BRANCH_SUMMARY_REPORT without REGION and LAST_AUDIT_DATE
            // val branchSummaryDF = createBranchSummaryReport(transactionDF, accountDF, branchDF)
            // writeToDeltaTable(branchSummaryDF, "BRANCH_SUMMARY_REPORT")

            // [ADDED] New logic for BRANCH_SUMMARY_REPORT with REGION and LAST_AUDIT_DATE
            val branchSummaryDF = createBranchSummaryReport(transactionDF, accountDF, branchDF, branchOperationalDF)
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

// [MODIFIED] All changes are annotated with // [ADDED], // [MODIFIED], or // [DEPRECATED] for traceability.
// [ADDED] REGION and LAST_AUDIT_DATE are only populated for active branches (IS_ACTIVE = 'Y').
// [ADDED] Legacy logic is preserved and commented out for reference.
