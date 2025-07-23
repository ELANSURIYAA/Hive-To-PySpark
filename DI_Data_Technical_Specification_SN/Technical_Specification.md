=============================================
Author: Ascendion AVA+
Date: <Leave it blank>
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS

## Introduction
This document outlines the technical specifications for integrating the new source table, BRANCH_OPERATIONAL_DETAILS, into the existing BRANCH_SUMMARY_REPORT table. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Modules
- Scala ETL logic
- Delta table structure
- Data validation and reconciliation routines

### Logic Changes
- Join BRANCH_OPERATIONAL_DETAILS with BRANCH_SUMMARY_REPORT using BRANCH_ID.
- Populate REGION and LAST_AUDIT_DATE columns conditionally based on IS_ACTIVE = 'Y'.

### Pseudocode
```scala
val branchOperationalDetails = spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "BRANCH_OPERATIONAL_DETAILS").load()
val branchSummaryReport = spark.read.format("delta").load("/path/to/branch_summary_report")

val updatedBranchSummaryReport = branchSummaryReport
  .join(branchOperationalDetails, "BRANCH_ID")
  .withColumn("REGION", when(col("IS_ACTIVE") === "Y", col("REGION")))
  .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === "Y", col("LAST_AUDIT_DATE")))

updatedBranchSummaryReport.write.format("delta").mode("overwrite").save("/path/to/branch_summary_report")
```

## Data Model Updates
### Source Data Model
#### BRANCH_OPERATIONAL_DETAILS
- BRANCH_ID: INT
- REGION: VARCHAR2(50)
- MANAGER_NAME: VARCHAR2(100)
- LAST_AUDIT_DATE: DATE
- IS_ACTIVE: CHAR(1)

### Target Data Model
#### BRANCH_SUMMARY_REPORT
- BRANCH_ID: INT
- BRANCH_NAME: STRING
- TOTAL_TRANSACTIONS: BIGINT
- TOTAL_AMOUNT: DOUBLE
- REGION: STRING (New Column)
- LAST_AUDIT_DATE: DATE (New Column)

## Source-to-Target Mapping
| Source Column                     | Target Column                     | Transformation Rule                  |
|-----------------------------------|-----------------------------------|--------------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION      | Populate if IS_ACTIVE = 'Y'          |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y' |

## Assumptions and Constraints
- The BRANCH_OPERATIONAL_DETAILS table is always up-to-date and accurate.
- Backward compatibility with older records is maintained.
- Full reload of BRANCH_SUMMARY_REPORT is required during deployment.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS
- Source Data Model: BRANCH_OPERATIONAL_DETAILS
- Target Data Model: BRANCH_SUMMARY_REPORT