=============================================
Author: Ascendion AVA+
Date: <Leave it blank>
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS

## Introduction
This document outlines the technical specifications for integrating the new source table `BRANCH_OPERATIONAL_DETAILS` into the existing `BRANCH_SUMMARY_REPORT` table. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Areas
- **Scala ETL Logic:**
  - Enhance the existing ETL pipeline to join the `BRANCH_OPERATIONAL_DETAILS` table using `BRANCH_ID`.
  - Populate the new columns `REGION` and `LAST_AUDIT_DATE` conditionally based on `IS_ACTIVE = 'Y'`.

### Logic Changes
- **Join Logic:**
  ```scala
  val branchOperationalDetails = spark.read.format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", "BRANCH_OPERATIONAL_DETAILS")
    .load()

  val updatedBranchSummary = branchSummary
    .join(branchOperationalDetails, "BRANCH_ID")
    .withColumn("REGION", when(col("IS_ACTIVE") === "Y", col("REGION")))
    .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === "Y", col("LAST_AUDIT_DATE")))
  ```

## Data Model Updates
### Source Data Model
The new source table `BRANCH_OPERATIONAL_DETAILS` includes the following schema:
- `BRANCH_ID` (Primary Key)
- `REGION`
- `MANAGER_NAME`
- `LAST_AUDIT_DATE`
- `IS_ACTIVE`

### Target Data Model
The `BRANCH_SUMMARY_REPORT` table will be updated to include:
- `REGION`
- `LAST_AUDIT_DATE`

## Source-to-Target Mapping
| Source Column                     | Target Column                     | Transformation Rules                  |
|-----------------------------------|-----------------------------------|---------------------------------------|
| `BRANCH_OPERATIONAL_DETAILS.REGION` | `BRANCH_SUMMARY_REPORT.REGION`     | Populate if `IS_ACTIVE = 'Y'`         |
| `BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE` | `BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE` | Populate if `IS_ACTIVE = 'Y'`         |

## Assumptions and Constraints
- The `BRANCH_OPERATIONAL_DETAILS` table is always up-to-date and accurate.
- The integration logic maintains backward compatibility with older records.
- Full reload of the `BRANCH_SUMMARY_REPORT` table is required post-deployment.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS
- Source DDL: `BRANCH_OPERATIONAL_DETAILS`
- Target DDL: `BRANCH_SUMMARY_REPORT`

=============================================