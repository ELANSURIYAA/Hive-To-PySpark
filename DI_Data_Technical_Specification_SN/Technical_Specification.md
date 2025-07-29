=============================================
Author: Ascendion AVA+
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for BRANCH_OPERATIONAL_DETAILS Integration

## Introduction
This document outlines the technical specifications for integrating the new source table, BRANCH_OPERATIONAL_DETAILS, into the existing BRANCH_SUMMARY_REPORT table. The integration aims to enhance compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Areas
- **Scala ETL Logic**: Enhance the existing Scala ETL pipeline to join BRANCH_OPERATIONAL_DETAILS using BRANCH_ID.
- **Delta Table Structure**: Update BRANCH_SUMMARY_REPORT schema to include new columns REGION and LAST_AUDIT_DATE.
- **Data Validation**: Modify validation routines to ensure accurate data mapping and reconciliation.

### Logic Changes
- **Join Condition**: Use BRANCH_ID to join BRANCH_OPERATIONAL_DETAILS with BRANCH_SUMMARY_REPORT.
- **Conditional Population**: Populate REGION and LAST_AUDIT_DATE columns only if IS_ACTIVE = 'Y'.

### Pseudocode
```scala
val branchOperationalDetails = spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "BRANCH_OPERATIONAL_DETAILS").load()
val branchSummaryReport = spark.read.format("delta").load("/delta/BRANCH_SUMMARY_REPORT")

val updatedReport = branchSummaryReport
  .join(branchOperationalDetails, Seq("BRANCH_ID"), "left")
  .withColumn("REGION", when(col("IS_ACTIVE") === "Y", col("REGION")))
  .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === "Y", col("LAST_AUDIT_DATE")))

updatedReport.write.format("delta").mode("overwrite").save("/delta/BRANCH_SUMMARY_REPORT")
```

## Data Model Updates
### Source Data Model
- **BRANCH_OPERATIONAL_DETAILS**
  - BRANCH_ID: INT
  - REGION: VARCHAR2(50)
  - MANAGER_NAME: VARCHAR2(100)
  - LAST_AUDIT_DATE: DATE
  - IS_ACTIVE: CHAR(1)

### Target Data Model
- **BRANCH_SUMMARY_REPORT**
  - Add REGION (STRING)
  - Add LAST_AUDIT_DATE (DATE)

## Source-to-Target Mapping
| Source Column                     | Target Column                     | Transformation Rule                  |
|-----------------------------------|-----------------------------------|---------------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION      | Populate if IS_ACTIVE = 'Y'          |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y' |

## Assumptions and Constraints
- **Assumptions**:
  - BRANCH_OPERATIONAL_DETAILS table is populated and accessible.
  - IS_ACTIVE column accurately reflects the operational status.
- **Constraints**:
  - Full reload of BRANCH_SUMMARY_REPORT is required.
  - Data governance and security standards must be adhered to.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS
- DDL Files: Source and Target Data Models

=============================================