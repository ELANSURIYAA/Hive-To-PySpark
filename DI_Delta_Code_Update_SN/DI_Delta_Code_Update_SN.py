=============================================
Author: Data Engineering Team - Regulatory Reporting
Date: 2025-04-23
Description: Spark Scala ETL job for AML and compliance reporting, enhanced to support new regulatory branch operational fields and schema evolution.
Updated by: ASCENDION AVA+
Updated on: 
Description: Adds REGION and LAST_AUDIT_DATE to BRANCH_SUMMARY_REPORT, with conditional population for active branches, and preserves legacy logic for backward compatibility.
=============================================

/*
* ----------------------------------------------------------------------------------------
* Script Name   : RegulatoryReportingETL.scala
* Created By    : Data Engineering Team - Regulatory Reporting
* Author        : <Your Name>
* Created Date  : 2025-04-23
* Description   : This Spark Scala job extracts data from Oracle DB for AML and compliance
*                 reporting, transforms it, and loads it into two Delta tables on Databricks:
*                 - AML_CUSTOMER_TRANSACTIONS
*                 - BRANCH_SUMMARY_REPORT
*                 [MODIFIED] Enhanced to support REGION and LAST_AUDIT_DATE fields as per new technical specifications.
* ----------------------------------------------------------------------------------------
* Updated by    : ASCENDION AVA+
* Updated on    : 
* Description   : Adds REGION and LAST_AUDIT_DATE to BRANCH_SUMMARY_REPORT, with conditional population for active branches, and preserves legacy logic for backward compatibility.
* ----------------------------------------------------------------------------------------
*/

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
      * [ADDED] Joins BRANCH_OPERATIONAL_DETAILS and applies conditional logic for REGION and LAST_AUDIT_DATE.
      * [MODIFIED] Preserves legacy logic for BRANCH_NAME, TOTAL_TRANSACTIONS, TOTAL_AMOUNT.
      * @param transactionDF Transaction DataFrame
      * @param accountDF Account DataFrame
      * @param branchDF Branch DataFrame
      * @param branchOperationalDF Branch Operational Details DataFrame
      * @return DataFrame for BRANCH_SUMMARY_REPORT
      */
    def createBranchSummaryReport(
        transactionDF: DataFrame,
        accountDF: DataFrame,
        branchDF: DataFrame,
        branchOperationalDF: DataFrame
    ): DataFrame = {
        val baseSummaryDF = transactionDF
            .join(accountDF, "ACCOUNT_ID")
            .join(branchDF, "BRANCH_ID")
            .groupBy("BRANCH_ID", "BRANCH_NAME")
            .agg(
                count("*").alias("TOTAL_TRANSACTIONS"),
                sum("AMOUNT").alias("TOTAL_AMOUNT")
            )

        // [ADDED] Join with BRANCH_OPERATIONAL_DETAILS for REGION and LAST_AUDIT_DATE
        val summaryWithOperational = baseSummaryDF
            .join(branchOperationalDF, Seq("BRANCH_ID"), "left")
            .withColumn(
                "REGION",
                when(col("IS_ACTIVE") === lit("Y"), col("REGION")).otherwise(lit(null)) // [ADDED] Only for active branches
            )
            .withColumn(
                "LAST_AUDIT_DATE",
                when(col("IS_ACTIVE") === lit("Y"), col("LAST_AUDIT_DATE")).otherwise(lit(null)) // [ADDED] Only for active branches
            )
            // [MODIFIED] Retain legacy columns as-is
            .select(
                col("BRANCH_ID"),
                col("BRANCH_NAME"),
                col("TOTAL_TRANSACTIONS"),
                col("TOTAL_AMOUNT"),
                col("REGION"),
                col("LAST_AUDIT_DATE")
            )
        summaryWithOperational
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

            // [MODIFIED] Create and write BRANCH_SUMMARY_REPORT with new fields
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

// [DEPRECATED] Previous version of createBranchSummaryReport without REGION and LAST_AUDIT_DATE
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
