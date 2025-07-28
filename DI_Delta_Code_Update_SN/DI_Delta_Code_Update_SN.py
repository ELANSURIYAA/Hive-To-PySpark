=============================================
Author: Data Engineering Team - Regulatory Reporting
Date: 2025-04-23
Description: Enhanced Spark Scala ETL for AML and compliance reporting, with schema evolution for BRANCH_SUMMARY_REPORT, conditional population of REGION and LAST_AUDIT_DATE, and backward compatibility.
Updated by: ASCENDION AVA+
Updated on: 
Description: This output adds logic to join BRANCH_SUMMARY_REPORT with BRANCH_OPERATIONAL_DETAILS, conditionally populates REGION and LAST_AUDIT_DATE for active branches, and annotates all changes for traceability.
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
      * [ADDED] Joins BRANCH_SUMMARY_REPORT with BRANCH_OPERATIONAL_DETAILS to add REGION and LAST_AUDIT_DATE.
      * Populates these fields only for active branches (IS_ACTIVE = 'Y').
      * Legacy logic for BRANCH_NAME, TOTAL_TRANSACTIONS, TOTAL_AMOUNT is preserved.
      */
    def createBranchSummaryReport(
        transactionDF: DataFrame,
        accountDF: DataFrame,
        branchDF: DataFrame,
        branchOperationalDF: DataFrame
    ): DataFrame = {
        // [ADDED] Aggregate transaction data by branch
        val summaryDF = transactionDF
            .join(accountDF, "ACCOUNT_ID")
            .join(branchDF, "BRANCH_ID")
            .groupBy("BRANCH_ID", "BRANCH_NAME")
            .agg(
                count("*").alias("TOTAL_TRANSACTIONS"),
                sum("AMOUNT").alias("TOTAL_AMOUNT")
            )

        // [ADDED] Join with BRANCH_OPERATIONAL_DETAILS for REGION and LAST_AUDIT_DATE
        val joinedDF = summaryDF
            .join(branchOperationalDF, Seq("BRANCH_ID"), "left")
            .withColumn(
                "REGION",
                when(col("IS_ACTIVE") === lit("Y"), col("REGION")).otherwise(lit(null))
            ) // [ADDED] Populate REGION only if IS_ACTIVE = 'Y'
            .withColumn(
                "LAST_AUDIT_DATE",
                when(col("IS_ACTIVE") === lit("Y"), col("LAST_AUDIT_DATE")).otherwise(lit(null))
            ) // [ADDED] Populate LAST_AUDIT_DATE only if IS_ACTIVE = 'Y'

        // [ADDED] Select columns in target schema order
        joinedDF.select(
            col("BRANCH_ID"),
            col("BRANCH_NAME"),
            col("TOTAL_TRANSACTIONS"),
            col("TOTAL_AMOUNT"),
            col("REGION"),
            col("LAST_AUDIT_DATE")
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
            val branchOperationalDF = readTable("BRANCH_OPERATIONAL_DETAILS") // [ADDED]

            // Create and write AML_CUSTOMER_TRANSACTIONS
            val amlTransactionsDF = createAmlCustomerTransactions(customerDF, accountDF, transactionDF)
            writeToDeltaTable(amlTransactionsDF, "AML_CUSTOMER_TRANSACTIONS")

            // [DEPRECATED] Old BRANCH_SUMMARY_REPORT logic (commented out for traceability)
            /*
            val branchSummaryDF = createBranchSummaryReport(transactionDF, accountDF, branchDF)
            writeToDeltaTable(branchSummaryDF, "BRANCH_SUMMARY_REPORT")
            */

            // [MODIFIED] New BRANCH_SUMMARY_REPORT logic with REGION and LAST_AUDIT_DATE
            val branchSummaryDF = createBranchSummaryReport(transactionDF, accountDF, branchDF, branchOperationalDF) // [MODIFIED]
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

// [MODIFIED] All changes are annotated inline as per technical specification.
// [ADDED] REGION and LAST_AUDIT_DATE are conditionally populated for active branches only.
// [DEPRECATED] Previous logic is commented out for traceability.
// [ADDED] Metadata header updated as per requirements.
