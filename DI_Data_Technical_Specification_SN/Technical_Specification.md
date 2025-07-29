=============================================
Author: Ascendion AVA+
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS

## Introduction
This document outlines the technical specifications for integrating the BRANCH_OPERATIONAL_DETAILS source table into the BRANCH_SUMMARY_REPORT target table. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Areas
- Scala ETL logic
- Delta table structure
- Data validation and reconciliation routines

### Logic Changes
1. **Join Logic:**
   - Join BRANCH_OPERATIONAL_DETAILS with BRANCH_SUMMARY_REPORT using BRANCH_ID.
2. **Conditional Population:**
   - Populate REGION and LAST_AUDIT_DATE columns based on IS_ACTIVE = 'Y'.

### Pseudocode
```scala
val branchOperationalDetails = spark.read.format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", "BRANCH_OPERATIONAL_DETAILS")
  .load()

val updatedBranchSummary = branchSummaryReport
  .join(branchOperationalDetails, Seq("BRANCH_ID"))
  .withColumn("REGION", when(col("IS_ACTIVE") === "Y", col("REGION")))
  .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === "Y", col("LAST_AUDIT_DATE")))

updatedBranchSummary.write.format("delta").mode("overwrite").save("/delta/branch_summary_report")
```

## Data Model Updates
### Source Data Model
**BRANCH_OPERATIONAL_DETAILS:**
- BRANCH_ID (INT)
- REGION (VARCHAR2(50))
- MANAGER_NAME (VARCHAR2(100))
- LAST_AUDIT_DATE (DATE)
- IS_ACTIVE (CHAR(1))

### Target Data Model
**BRANCH_SUMMARY_REPORT:**
- BRANCH_ID (INT)
- BRANCH_NAME (STRING)
- TOTAL_TRANSACTIONS (BIGINT)
- TOTAL_AMOUNT (DOUBLE)
- REGION (STRING) *(New)*
- LAST_AUDIT_DATE (DATE) *(New)*

## Source-to-Target Mapping
| Source Column                     | Target Column                     | Transformation Rule                  |
|-----------------------------------|-----------------------------------|--------------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION      | Populate if IS_ACTIVE = 'Y'          |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y' |

## Assumptions and Constraints
- **Assumptions:**
  - All branch IDs in BRANCH_OPERATIONAL_DETAILS exist in BRANCH_SUMMARY_REPORT.
  - IS_ACTIVE column accurately reflects active branches.
- **Constraints:**
  - Full reload of BRANCH_SUMMARY_REPORT required.
  - Ensure backward compatibility with older records.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS
- Source DDL: BRANCH_OPERATIONAL_DETAILS
- Target DDL: BRANCH_SUMMARY_REPORT