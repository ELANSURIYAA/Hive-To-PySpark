=============================================
Author: Ascendion AVA+
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS

## Introduction
This document outlines the technical specifications for integrating the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing `BRANCH_SUMMARY_REPORT` table within the ETL pipeline.

### Business Requirement
The integration aims to enhance compliance and audit readiness by incorporating branch-level operational metadata into the reporting layer.

## Code Changes
### Impacted Areas
- **Scala ETL Logic**: Enhance the existing logic to join `BRANCH_OPERATIONAL_DETAILS` using `BRANCH_ID`.
- **Delta Table Structure**: Update `BRANCH_SUMMARY_REPORT` schema to include new columns `REGION` and `LAST_AUDIT_DATE`.
- **Data Validation**: Modify validation routines to include new fields.

### Logic Changes
1. **Join Logic**:
   ```scala
   val branchDetails = spark.read.format("jdbc").options(Map(
       "url" -> jdbcUrl,
       "dbtable" -> "BRANCH_OPERATIONAL_DETAILS",
       "user" -> dbUser,
       "password" -> dbPassword
   )).load()

   val updatedReport = existingReport
       .join(branchDetails, Seq("BRANCH_ID"), "left")
       .withColumn("REGION", when(col("IS_ACTIVE") === "Y", col("REGION")))
       .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") === "Y", col("LAST_AUDIT_DATE")))
   ```

2. **Backward Compatibility**:
   Ensure older records remain unaffected.

## Data Model Updates
### Source Data Model
- **New Table**: `BRANCH_OPERATIONAL_DETAILS`
  - Columns: `BRANCH_ID`, `REGION`, `MANAGER_NAME`, `LAST_AUDIT_DATE`, `IS_ACTIVE`

### Target Data Model
- **Updated Table**: `BRANCH_SUMMARY_REPORT`
  - Added Columns: `REGION`, `LAST_AUDIT_DATE`

## Source-to-Target Mapping
| Source Column                     | Target Column                     | Transformation Rule                     |
|-----------------------------------|-----------------------------------|-----------------------------------------|
| `BRANCH_OPERATIONAL_DETAILS.REGION` | `BRANCH_SUMMARY_REPORT.REGION` | Populate only if `IS_ACTIVE = 'Y'`     |
| `BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE` | `BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE` | Populate only if `IS_ACTIVE = 'Y'`     |

## Assumptions and Constraints
- **Assumptions**:
  - `IS_ACTIVE` determines whether the metadata is included.
- **Constraints**:
  - Full reload of `BRANCH_SUMMARY_REPORT` is required.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS

---