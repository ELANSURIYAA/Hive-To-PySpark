=============================================
Author: Ascendion AVA+
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT

## Introduction
This document outlines the technical specifications for integrating the new source table `BRANCH_OPERATIONAL_DETAILS` into the `BRANCH_SUMMARY_REPORT` table. The integration aims to enhance compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Areas
- **Scala ETL Logic**: Enhance the existing pipeline to join the new `BRANCH_OPERATIONAL_DETAILS` table using `BRANCH_ID`.
- **Delta Table Structure**: Update the schema of `BRANCH_SUMMARY_REPORT` to include two new columns: `REGION` and `LAST_AUDIT_DATE`.
- **Data Validation and Reconciliation**: Modify routines to validate and reconcile the new data.

### Logic Changes
1. **Join Logic**:
   - Join `BRANCH_OPERATIONAL_DETAILS` with `BRANCH_SUMMARY_REPORT` on `BRANCH_ID`.
   - Populate `REGION` and `LAST_AUDIT_DATE` conditionally based on `IS_ACTIVE = 'Y'`.

2. **Backward Compatibility**:
   - Ensure older records in `BRANCH_SUMMARY_REPORT` remain unaffected.

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

updatedBranchSummary.write.format("delta").mode("overwrite").save("/delta/BRANCH_SUMMARY_REPORT")
```

## Data Model Updates
### Source Data Model
- **BRANCH_OPERATIONAL_DETAILS**:
  - New table with columns: `BRANCH_ID`, `REGION`, `MANAGER_NAME`, `AUDIT_DATE`, `IS_ACTIVE`.

### Target Data Model
- **BRANCH_SUMMARY_REPORT**:
  - Add columns:
    - `REGION` (String)
    - `LAST_AUDIT_DATE` (Date)

## Source-to-Target Mapping
| Source Column                  | Target Column                  | Transformation Rule                   |
|--------------------------------|--------------------------------|---------------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION | Populate if `IS_ACTIVE = 'Y'`         |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if `IS_ACTIVE = 'Y'` |

## Assumptions and Constraints
- **Assumptions**:
  - `BRANCH_OPERATIONAL_DETAILS` table is populated and accessible.
  - `IS_ACTIVE` column accurately reflects active status.
- **Constraints**:
  - Full reload of `BRANCH_SUMMARY_REPORT` is required.
  - Data governance standards must be adhered to.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
- Source and Target Data Models

=============================================
Cost Estimation and Justification
=============================================
- **Input Tokens**: Calculated based on the prompt and input files.
- **Output Tokens**: Calculated based on the technical specification content.
- **Model Used**: GPT-4.
- **Pricing**:
  - Input Cost = `input_tokens * [input_cost_per_token]`
  - Output Cost = `output_tokens * [output_cost_per_token]`

Total Cost: [Calculated based on the above formula]
=============================================