=============================================
Author: Data Engineering Team - Regulatory Reporting
Updated by: ASCENDION AVA+
Date: 
Updated on: 
Description: Spark Scala ETL for AML and compliance reporting, enhanced to join branch operational details and conditionally populate REGION and LAST_AUDIT_DATE in BRANCH_SUMMARY_REPORT.
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

    // [MODIFIED] Enhanced: Create BRANCH_SUMMARY_REPORT DataFrame with REGION and LAST_AUDIT_DATE
    /**
     * [MODIFIED]
     * Now joins with BRANCH_OPERATIONAL_DETAILS and conditionally populates REGION and LAST_AUDIT_DATE
     * for active branches (IS_ACTIVE = 'Y').
     * Preserves legacy logic for BRANCH_NAME, TOTAL_TRANSACTIONS, TOTAL_AMOUNT.
     */
    def createBranchSummaryReport(
        transactionDF: DataFrame,
        accountDF: DataFrame,
        branchDF: DataFrame,
        branchOperationalDF: DataFrame // [ADDED]
    ): DataFrame = {
        val baseDF = transactionDF
            .join(accountDF, "ACCOUNT_ID")
            .join(branchDF, "BRANCH_ID")
            .groupBy("BRANCH_ID", "BRANCH_NAME")
            .agg(
                count("*").alias("TOTAL_TRANSACTIONS"),
                sum("AMOUNT").alias("TOTAL_AMOUNT")
            )

        // [ADDED] Join with BRANCH_OPERATIONAL_DETAILS for REGION and LAST_AUDIT_DATE
        val joinedDF = baseDF
            .join(branchOperationalDF, Seq("BRANCH_ID"), "left")
            .withColumn(
                "REGION",
                when(col("IS_ACTIVE") === lit("Y"), col("REGION")).otherwise(lit(null)) // [ADDED]
            )
            .withColumn(
                "LAST_AUDIT_DATE",
                when(col("IS_ACTIVE") === lit("Y"), col("LAST_AUDIT_DATE")).otherwise(lit(null)) // [ADDED]
            )
            // [MODIFIED] Only keep relevant columns for output schema
            .select(
                col("BRANCH_ID"),
                col("BRANCH_NAME"),
                col("TOTAL_TRANSACTIONS"),
                col("TOTAL_AMOUNT"),
                col("REGION"), // [ADDED]
                col("LAST_AUDIT_DATE") // [ADDED]
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

            // [MODIFIED] Create and write BRANCH_SUMMARY_REPORT with enhanced logic
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

// [MODIFIED] BRANCH_SUMMARY_REPORT now includes REGION and LAST_AUDIT_DATE, conditionally populated for active branches.
// [ADDED] Joins with BRANCH_OPERATIONAL_DETAILS for new fields.
// [MODIFIED] All changes are annotated with [ADDED] or [MODIFIED] for traceability.
