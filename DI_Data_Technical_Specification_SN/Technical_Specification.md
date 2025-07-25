=============================================
Author: Ascendion AVA+
Date: <Leave it blank>
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT

## Introduction
This document outlines the technical specifications for integrating the new Oracle source table BRANCH_OPERATIONAL_DETAILS into the existing Databricks Delta table BRANCH_SUMMARY_REPORT. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Areas
- Scala ETL logic
- Delta table structure
- Data validation and reconciliation routines

### Logic Enhancements
1. **Join Logic:**
   - Join BRANCH_OPERATIONAL_DETAILS with BRANCH_SUMMARY_REPORT using BRANCH_ID.
   - Populate REGION and LAST_AUDIT_DATE conditionally based on IS_ACTIVE = 'Y'.

2. **Backward Compatibility:**
   - Ensure older records remain unaffected.

### Pseudocode
```scala
val branchOperationalDetails = spark.read.format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", "BRANCH_OPERATIONAL_DETAILS")
  .load()

val branchSummaryReport = spark.read.format("delta").load("/delta/BRANCH_SUMMARY_REPORT")

val updatedReport = branchSummaryReport
  .join(branchOperationalDetails, "BRANCH_ID")
  .withColumn("REGION", when(col("IS_ACTIVE") === "Y", col("REGION")))
  .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === "Y", col("LAST_AUDIT_DATE")))

updatedReport.write.format("delta").mode("overwrite").save("/delta/BRANCH_SUMMARY_REPORT")
```

## Data Model Updates
### Source Data Model
#### BRANCH_OPERATIONAL_DETAILS
- **BRANCH_ID:** INT (Primary Key)
- **REGION:** VARCHAR2(50)
- **LAST_AUDIT_DATE:** DATE
- **IS_ACTIVE:** CHAR(1)

### Target Data Model
#### BRANCH_SUMMARY_REPORT
- Add Columns:
  - **REGION:** STRING
  - **LAST_AUDIT_DATE:** DATE

## Source-to-Target Mapping
| Source Column                     | Target Column                     | Transformation Rule                          |
|-----------------------------------|-----------------------------------|---------------------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION      | Populate if IS_ACTIVE = 'Y'                 |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y' |

## Assumptions and Constraints
- Full reload of BRANCH_SUMMARY_REPORT is required.
- Data governance and security standards must be adhered to.
- Compatibility with existing systems and processes must be maintained.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
- Source DDL
- Target DDL