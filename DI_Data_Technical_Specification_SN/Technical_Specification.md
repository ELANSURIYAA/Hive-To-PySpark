=============================================
Author: Ascendion AVA+
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS

## Introduction
This document outlines the technical specifications for integrating the new source table BRANCH_OPERATIONAL_DETAILS into the BRANCH_SUMMARY_REPORT table. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Areas
- **Scala ETL Logic**: Enhance the existing logic to join BRANCH_OPERATIONAL_DETAILS using BRANCH_ID.
- **Delta Table Structure**: Update BRANCH_SUMMARY_REPORT schema to include REGION and LAST_AUDIT_DATE.
- **Data Validation**: Modify reconciliation routines to validate the new columns.

### Logic Changes
- **Join Condition**: `BRANCH_OPERATIONAL_DETAILS.BRANCH_ID = BRANCH_SUMMARY_REPORT.BRANCH_ID`
- **Conditional Population**:
  - Populate REGION and LAST_AUDIT_DATE only if `IS_ACTIVE = 'Y'`.

### Pseudocode
```scala
val branchOperationalDetails = spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "BRANCH_OPERATIONAL_DETAILS").load()
val branchSummaryReport = branchOperationalDetails
  .filter("IS_ACTIVE = 'Y'")
  .join(branchSummaryReport, Seq("BRANCH_ID"))
  .select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE")
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
*(Include a diagram showing the relationship between BRANCH_OPERATIONAL_DETAILS and BRANCH_SUMMARY_REPORT)*

## Source-to-Target Mapping
| Source Column                     | Target Column                     | Transformation Rule             |
|-----------------------------------|-----------------------------------|----------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION      | Direct Mapping                  |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Direct Mapping                  |

## Assumptions and Constraints
- **Backward Compatibility**: Older records in BRANCH_SUMMARY_REPORT must remain unaffected.
- **Deployment**: Requires a full reload of BRANCH_SUMMARY_REPORT.
- **Data Governance**: Ensure compliance with data security standards.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Context: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS
- Source DDL: Provided schema for BRANCH_OPERATIONAL_DETAILS
- Target DDL: Provided schema for BRANCH_SUMMARY_REPORT