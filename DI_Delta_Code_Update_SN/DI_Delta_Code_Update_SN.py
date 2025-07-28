// =============================================
// Author: Data Engineering Team - Regulatory Reporting
// Updated by: ASCENDION AVA+
// Updated on: 
// Description: Enhanced Spark Scala ETL for AML and compliance reporting.
//              Adds conditional REGION and LAST_AUDIT_DATE fields to BRANCH_SUMMARY_REPORT,
//              joins with BRANCH_OPERATIONAL_DETAILS, and preserves legacy logic with clear annotations.
// =============================================

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
  val logger: Logger = Logger.getLogger("RegulatoryReportingETL")

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

  // [ADDED] Read BRANCH_OPERATIONAL_DETAILS table
  def readBranchOperationalDetails(): DataFrame = readTable("BRANCH_OPERATIONAL_DETAILS")

  // [MODIFIED] Create BRANCH_SUMMARY_REPORT DataFrame with new fields and logic
  def createBranchSummaryReport(
    transactionDF: DataFrame,
    accountDF: DataFrame,
    branchDF: DataFrame,
    branchOpDF: DataFrame
  ): DataFrame = {

    // Legacy summary
    val summaryDF = transactionDF
      .join(accountDF, "ACCOUNT_ID")
      .join(branchDF, "BRANCH_ID")
      .groupBy("BRANCH_ID", "BRANCH_NAME")
      .agg(
        count("*").alias("TOTAL_TRANSACTIONS"),
        sum("AMOUNT").alias("TOTAL_AMOUNT")
      )

    // [ADDED] Join with BRANCH_OPERATIONAL_DETAILS for REGION and LAST_AUDIT_DATE
    val joinedDF = summaryDF.join(branchOpDF, Seq("BRANCH_ID"), "left")

    // [ADDED] Conditional population for REGION and LAST_AUDIT_DATE
    val resultDF = joinedDF
      .withColumn("REGION", when(col("IS_ACTIVE") === "Y", col("REGION")).otherwise(lit(null)))
      .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === "Y", col("LAST_AUDIT_DATE")).otherwise(lit(null)))

    resultDF.select("BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE")
  }

  // Write DataFrame to Delta table
  def writeToDeltaTable(df: DataFrame, tableName: String): Unit = {
    df.write.format("delta").mode("overwrite").saveAsTable(tableName)
    logger.info(s"Written to $tableName")
  }

  // [ADDED] Pre-load validation checks
  def validateBranchIds(branchOpDF: DataFrame, summaryDF: DataFrame): Unit = {
    val missing = branchOpDF.join(summaryDF, Seq("BRANCH_ID"), "left_anti")
    val countMissing = missing.count()
    if (countMissing > 0) {
      logger.warn(s"$countMissing BRANCH_IDs in BRANCH_OPERATIONAL_DETAILS not found in BRANCH_SUMMARY_REPORT")
    }
  }

  // [ADDED] IS_ACTIVE value normalization
  def normalizeIsActive(branchOpDF: DataFrame): DataFrame = {
    branchOpDF.withColumn("IS_ACTIVE", when(col("IS_ACTIVE") === "Y", "Y").otherwise("N"))
  }

  // [ADDED] Post-load validation checks
  def postLoadValidation(resultDF: DataFrame): Unit = {
    val activeBranches = resultDF.filter(col("REGION").isNotNull && col("LAST_AUDIT_DATE").isNotNull)
    logger.info(s"Active branches with REGION and LAST_AUDIT_DATE populated: ${activeBranches.count()}")
  }

  // Main ETL execution
  def main(args: Array[String]): Unit = {
    try {
      val customerDF = readTable("CUSTOMER")
      val accountDF = readTable("ACCOUNT")
      val transactionDF = readTable("TRANSACTION")
      val branchDF = readTable("BRANCH")
      var branchOpDF = readBranchOperationalDetails() // [ADDED]

      // [ADDED] Normalize IS_ACTIVE values
      branchOpDF = normalizeIsActive(branchOpDF)

      // [ADDED] Pre-load validation
      val summaryDF = transactionDF
        .join(accountDF, "ACCOUNT_ID")
        .join(branchDF, "BRANCH_ID")
        .groupBy("BRANCH_ID", "BRANCH_NAME")
        .agg(count("*").alias("TOTAL_TRANSACTIONS"), sum("AMOUNT").alias("TOTAL_AMOUNT"))
      validateBranchIds(branchOpDF, summaryDF)

      // Create and write AML_CUSTOMER_TRANSACTIONS (legacy logic preserved)
      val amlTransactionsDF = customerDF
        .join(accountDF, "CUSTOMER_ID")
        .join(transactionDF, "ACCOUNT_ID")
        .select("CUSTOMER_ID", "NAME", "ACCOUNT_ID", "TRANSACTION_ID", "AMOUNT", "TRANSACTION_TYPE", "TRANSACTION_DATE")
      writeToDeltaTable(amlTransactionsDF, "AML_CUSTOMER_TRANSACTIONS")

      // [MODIFIED] Create and write BRANCH_SUMMARY_REPORT with new logic
      val branchSummaryDF = createBranchSummaryReport(transactionDF, accountDF, branchDF, branchOpDF)
      writeToDeltaTable(branchSummaryDF, "BRANCH_SUMMARY_REPORT")

      // [ADDED] Post-load validation
      postLoadValidation(branchSummaryDF)

    } catch {
      case e: Exception =>
        logger.error(s"ETL job failed with exception: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
      logger.info("Spark session stopped.")
    }
  }
}
