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

    // Create BRANCH_SUMMARY_REPORT DataFrame
    def createBranchSummaryReport(transactionDF: DataFrame, accountDF: DataFrame, branchDF: DataFrame): DataFrame = {
        transactionDF
            .join(accountDF, "ACCOUNT_ID")
            .join(branchDF, "BRANCH_ID")
            .groupBy("BRANCH_ID", "BRANCH_NAME")
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

            // Create and write AML_CUSTOMER_TRANSACTIONS
            val amlTransactionsDF = createAmlCustomerTransactions(customerDF, accountDF, transactionDF)
            writeToDeltaTable(amlTransactionsDF, "AML_CUSTOMER_TRANSACTIONS")

            // Create and write BRANCH_SUMMARY_REPORT
            val branchSummaryDF = createBranchSummaryReport(transactionDF, accountDF, branchDF)
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
