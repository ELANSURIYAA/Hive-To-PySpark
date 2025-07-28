=============================================
Author: Ascendion AVA+
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS

## Introduction
This document outlines the technical specification for integrating the BRANCH_OPERATIONAL_DETAILS table into the BRANCH_SUMMARY_REPORT table. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Areas
- **Scala ETL Logic:**
  - Enhance the existing Scala ETL pipeline to join BRANCH_OPERATIONAL_DETAILS with BRANCH_SUMMARY_REPORT using BRANCH_ID.
  - Populate REGION and LAST_AUDIT_DATE columns conditionally based on IS_ACTIVE = 'Y'.

### Logic Changes
- **Join Logic:**
  ```scala
  val branchOperationalDetails = spark.read.format("jdbc").option("url", jdbcUrl)
    .option("dbtable", "BRANCH_OPERATIONAL_DETAILS").load()

  val updatedBranchSummary = branchSummary.join(branchOperationalDetails, "BRANCH_ID")
    .withColumn("REGION", when(col("IS_ACTIVE") === "Y", col("REGION")))
    .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === "Y", col("LAST_AUDIT_DATE")))
  ```

## Data Model Updates
### Source Data Model
- **BRANCH_OPERATIONAL_DETAILS:**
  - New columns: REGION, LAST_AUDIT_DATE

### Target Data Model
- **BRANCH_SUMMARY_REPORT:**
  - Add REGION (STRING)
  - Add LAST_AUDIT_DATE (DATE)

## Source-to-Target Mapping
| Source Column                     | Target Column                     | Transformation Rule                |
|-----------------------------------|-----------------------------------|------------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION      | Populate if IS_ACTIVE = 'Y'        |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y' |

## Assumptions and Constraints
- **Assumptions:**
  - BRANCH_OPERATIONAL_DETAILS contains accurate and up-to-date metadata.
  - IS_ACTIVE flag correctly represents the branch's operational status.

- **Constraints:**
  - Full reload of BRANCH_SUMMARY_REPORT is required post-deployment.
  - Ensure backward compatibility with older records.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS
- DDL Files: Source and Target Data Models

=============================================