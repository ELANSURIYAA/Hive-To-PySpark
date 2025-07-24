=============================================
Author: Ascendion AVA+
Date: <Leave it blank>
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS

## Introduction
This document outlines the technical specifications for integrating the new source table `BRANCH_OPERATIONAL_DETAILS` into the target table `BRANCH_SUMMARY_REPORT`. The enhancement is driven by compliance and audit requirements.

## Code Changes
### Impacted Areas
- Scala ETL logic
- Delta table structure
- Data validation and reconciliation routines

### Required Changes
1. **Schema Update:**
   - Alter `BRANCH_SUMMARY_REPORT` to add `REGION STRING` and `LAST_AUDIT_DATE DATE`.

2. **ETL Logic:**
   - Enhance the Scala ETL job to:
     - LEFT JOIN `BRANCH_OPERATIONAL_DETAILS` ON `BRANCH_ID`
     - Populate `REGION` and `LAST_AUDIT_DATE` only if `IS_ACTIVE = 'Y'`, else set to NULL.
   - Ensure backward compatibility for existing records.

3. **Data Validation:**
   - Validate that for each record in `BRANCH_SUMMARY_REPORT`, the new columns are only populated where the source branch is active.
   - Reconcile counts of active branches with non-NULL values in the new columns.

## Data Model Updates
### Existing Source Data Model
Refer to the provided DDL file for the schema of `BRANCH_OPERATIONAL_DETAILS`.

### Existing Target Data Model
Refer to the provided DDL file for the schema of `BRANCH_SUMMARY_REPORT`.

### Updates
- Add `REGION` and `LAST_AUDIT_DATE` columns to `BRANCH_SUMMARY_REPORT`.

## Source-to-Target Mapping
### Mapping and Transformation Rules
| Target Column      | Source Table                | Source Column      | Transformation Rule                                                                 |
|--------------------|----------------------------|--------------------|-------------------------------------------------------------------------------------|
| BRANCH_ID          | Existing logic              | BRANCH_ID          | As per existing join logic                                                          |
| BRANCH_NAME        | Existing logic              | BRANCH_NAME        | As per existing logic                                                               |
| TOTAL_TRANSACTIONS | Existing logic              | N/A                | As per existing logic                                                               |
| TOTAL_AMOUNT       | Existing logic              | N/A                | As per existing logic                                                               |
| REGION             | BRANCH_OPERATIONAL_DETAILS  | REGION             | If IS_ACTIVE = 'Y' for branch, set REGION; else NULL                                |
| LAST_AUDIT_DATE    | BRANCH_OPERATIONAL_DETAILS  | LAST_AUDIT_DATE    | If IS_ACTIVE = 'Y' for branch, set LAST_AUDIT_DATE; else NULL                       |

### Example Mapping
| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION   | LAST_AUDIT_DATE |
|-----------|-------------|--------------------|--------------|----------|-----------------|
| 1001      | Midtown     | 120                | 500000.00    | North    | 2024-03-15      |
| 1002      | Uptown      | 80                 | 320000.00    | NULL     | NULL            |

## Assumptions and Constraints
- Only rows in `BRANCH_OPERATIONAL_DETAILS` where `IS_ACTIVE = 'Y'` will provide values for `REGION` and `LAST_AUDIT_DATE`.
- Backward compatibility: For historical records where no matching or active operational details exist, the new columns will be NULL.

## References
- [JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table]
- [Confluence: Business and Technical Requirements for Branch Operational Integration]