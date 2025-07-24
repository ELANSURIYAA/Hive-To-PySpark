=============================================
Author: Ascendion AVA+
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT.
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS

## Introduction
This document outlines the technical specifications for integrating the BRANCH_OPERATIONAL_DETAILS source table into the BRANCH_SUMMARY_REPORT target table. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Modules
- **Scala ETL Logic**: Update the existing Scala processing logic to join the BRANCH_OPERATIONAL_DETAILS table using BRANCH_ID.

### Logic and Functionality Changes
- Add conditional logic to populate REGION and LAST_AUDIT_DATE columns based on IS_ACTIVE = 'Y'.
- Ensure backward compatibility with older records.

### Pseudocode
```scala
val branchOperationalDetails = spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "BRANCH_OPERATIONAL_DETAILS").load()

val updatedBranchSummary = branchSummaryReport
  .join(branchOperationalDetails, Seq("BRANCH_ID"), "left")
  .withColumn("REGION", when(col("IS_ACTIVE") === "Y", col("REGION")))
  .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === "Y", col("LAST_AUDIT_DATE")))

updatedBranchSummary.write.format("delta").save("/delta/BRANCH_SUMMARY_REPORT")
```

## Data Model Updates
### Source Data Model
- **BRANCH_OPERATIONAL_DETAILS**:
  - REGION: VARCHAR2(50)
  - LAST_AUDIT_DATE: DATE

### Target Data Model
- **BRANCH_SUMMARY_REPORT**:
  - REGION: STRING
  - LAST_AUDIT_DATE: DATE

### Diagram
*(Include a diagram if applicable)*

## Source-to-Target Mapping
| Source Column                     | Target Column                     | Transformation Rule                |
|-----------------------------------|-----------------------------------|-------------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION      | Populate if IS_ACTIVE = 'Y'        |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y'        |

## Assumptions and Constraints
- Full reload of BRANCH_SUMMARY_REPORT is required.
- Data governance and security standards must be adhered to.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS

=============================================