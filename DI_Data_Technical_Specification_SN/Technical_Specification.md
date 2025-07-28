=============================================
Author: Ascendion AVA+
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS

## Introduction
This document outlines the technical specifications for integrating the new source table BRANCH_OPERATIONAL_DETAILS into the existing BRANCH_SUMMARY_REPORT. The integration aims to enhance compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Areas
- **Scala ETL Logic**: Enhance the existing ETL pipeline to join the BRANCH_OPERATIONAL_DETAILS table using BRANCH_ID.
- **Conditional Logic**: Populate REGION and LAST_AUDIT_DATE columns based on IS_ACTIVE = 'Y'.

### Pseudocode
```scala
val branchOperationalDetails = spark.read.format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", "BRANCH_OPERATIONAL_DETAILS")
  .load()

val branchSummaryReport = existingData.join(branchOperationalDetails, "BRANCH_ID")
  .withColumn("REGION", when(col("IS_ACTIVE") === "Y", col("REGION")))
  .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === "Y", col("LAST_AUDIT_DATE")))

branchSummaryReport.write.format("delta").mode("overwrite").save("/delta/BRANCH_SUMMARY_REPORT")
```

## Data Model Updates
### Source Data Model
**BRANCH_OPERATIONAL_DETAILS**:
- BRANCH_ID (INT, Primary Key)
- REGION (VARCHAR2(50))
- MANAGER_NAME (VARCHAR2(100))
- LAST_AUDIT_DATE (DATE)
- IS_ACTIVE (CHAR(1))

### Target Data Model
**BRANCH_SUMMARY_REPORT**:
- Add REGION (STRING)
- Add LAST_AUDIT_DATE (DATE)

## Source-to-Target Mapping
| Source Column                  | Target Column                  | Transformation Rule                |
|--------------------------------|--------------------------------|-------------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION | Populate if IS_ACTIVE = 'Y'        |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y' |

## Assumptions and Constraints
- **Backward Compatibility**: Ensure older records in BRANCH_SUMMARY_REPORT remain unaffected.
- **Full Reload**: A full reload of BRANCH_SUMMARY_REPORT is required post-deployment.
- **Data Governance**: Adhere to data security and compliance standards.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Context: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS
- Source DDL: BRANCH_OPERATIONAL_DETAILS
- Target DDL: BRANCH_SUMMARY_REPORT

=============================================