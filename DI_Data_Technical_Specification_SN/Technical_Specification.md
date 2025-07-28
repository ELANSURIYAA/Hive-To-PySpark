=============================================
Author: Ascendion AVA+
Date: 
Description: Technical Specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS

## Introduction
This document outlines the technical changes required to integrate the BRANCH_OPERATIONAL_DETAILS table into the BRANCH_SUMMARY_REPORT table. The integration aims to enhance compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Modules
- Scala ETL logic for BRANCH_SUMMARY_REPORT
- Data validation and reconciliation routines

### Logic Changes
1. **Join Logic**:
   - Join BRANCH_OPERATIONAL_DETAILS with BRANCH_SUMMARY_REPORT using BRANCH_ID.
2. **Conditional Population**:
   - Populate REGION and LAST_AUDIT_DATE columns based on IS_ACTIVE = 'Y'.

### Pseudocode
```scala
val branchOperationalDetails = spark.read.format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", "BRANCH_OPERATIONAL_DETAILS")
  .load()

val updatedBranchSummary = branchSummaryReport
  .join(branchOperationalDetails, "BRANCH_ID")
  .withColumn("REGION", when(col("IS_ACTIVE") === "Y", col("REGION")))
  .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === "Y", col("LAST_AUDIT_DATE")))
```

## Data Model Updates
### Source Data Model
- **BRANCH_OPERATIONAL_DETAILS**
  - REGION: VARCHAR2(50)
  - LAST_AUDIT_DATE: DATE
  - IS_ACTIVE: CHAR(1)

### Target Data Model
- **BRANCH_SUMMARY_REPORT**
  - Add REGION (STRING)
  - Add LAST_AUDIT_DATE (DATE)

## Source-to-Target Mapping
| Source Column                     | Target Column                     | Transformation Rule                  |
|-----------------------------------|-----------------------------------|--------------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION      | Populate if IS_ACTIVE = 'Y'          |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y' |

## Assumptions and Constraints
- Full reload of BRANCH_SUMMARY_REPORT is required.
- Backward compatibility with older records must be maintained.
- Data governance and security standards must be adhered to.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS
- Source Data Model: BRANCH_OPERATIONAL_DETAILS
- Target Data Model: BRANCH_SUMMARY_REPORT

=============================================