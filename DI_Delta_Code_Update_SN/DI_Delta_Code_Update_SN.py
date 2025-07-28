=============================================
Author: Data Engineering Team - Regulatory Reporting
Date: 
Description: Enhanced ETL job for regulatory reporting with schema evolution, conditional field population, and backward compatibility.
Updated by: ASCENDION AVA+
Updated on: 
Description: This output updates the ETL logic to join BRANCH_OPERATIONAL_DETAILS, conditionally populate REGION and LAST_AUDIT_DATE, and annotate all changes for traceability.
=============================================

'''
[MODIFIED] - Metadata header updated and extended with new fields as per requirements.
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

    // [MODIFIED] - Enhanced BRANCH_SUMMARY_REPORT logic to join with BRANCH_OPERATIONAL_DETAILS and conditionally populate REGION and LAST_AUDIT_DATE
    def createBranchSummaryReport(transactionDF: DataFrame, accountDF: DataFrame, branchDF: DataFrame, branchOpDF: DataFrame): DataFrame = {
        val baseDF = transactionDF
            .join(accountDF, "ACCOUNT_ID")
            .join(branchDF, "BRANCH_ID")
            .groupBy("BRANCH_ID", "BRANCH_NAME")
            .agg(
                count("*").alias("TOTAL_TRANSACTIONS"),
                sum("AMOUNT").alias("TOTAL_AMOUNT")
            )

        // [ADDED] - Join with BRANCH_OPERATIONAL_DETAILS
        val joinedDF = baseDF.join(branchOpDF, Seq("BRANCH_ID"), "left")

        // [ADDED] - Conditionally populate REGION and LAST_AUDIT_DATE only if IS_ACTIVE = 'Y'
        joinedDF.withColumn(
            "REGION",
            when(col("IS_ACTIVE") === lit("Y"), col("REGION")).otherwise(lit(null))
        ).withColumn(
            "LAST_AUDIT_DATE",
            when(col("IS_ACTIVE") === lit("Y"), col("LAST_AUDIT_DATE")).otherwise(lit(null))
        )
        // [MODIFIED] - Select final columns including new fields
        .select(
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
            val branchOpDF = readTable("BRANCH_OPERATIONAL_DETAILS") // [ADDED]

            // Create and write AML_CUSTOMER_TRANSACTIONS
            val amlTransactionsDF = createAmlCustomerTransactions(customerDF, accountDF, transactionDF)
            writeToDeltaTable(amlTransactionsDF, "AML_CUSTOMER_TRANSACTIONS")

            // [MODIFIED] - Create and write BRANCH_SUMMARY_REPORT with enhanced logic
            val branchSummaryDF = createBranchSummaryReport(transactionDF, accountDF, branchDF, branchOpDF)
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

'''
[ADDED] - All changes are annotated with [ADDED] or [MODIFIED] for traceability.
[MODIFIED] - Legacy logic for BRANCH_NAME, TOTAL_TRANSACTIONS, and TOTAL_AMOUNT is preserved.
[ADDED] - REGION and LAST_AUDIT_DATE are nullable for backward compatibility.
[ADDED] - Inline documentation and comments for new/modified logic.
[MODIFIED] - Ready for compilation and deployment.
'''
