=============================================
Author: Ascendion AVA+
Date: 
Description: Technical Specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT

## Introduction
This document outlines the technical specifications for integrating the new source table `BRANCH_OPERATIONAL_DETAILS` into the existing `BRANCH_SUMMARY_REPORT` table. The integration is aimed at enhancing regulatory reporting and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Areas
- **Scala ETL Logic:** Update the join logic to include `BRANCH_OPERATIONAL_DETAILS`.
- **Delta Table Structure:** Add new columns `REGION` and `LAST_AUDIT_DATE`.
- **Data Validation and Reconciliation:** Update routines to account for new columns.

### Pseudocode
```scala
val activeBranchDetails = branchOperationalDetails
  .filter($"IS_ACTIVE" === "Y")
  .select($"BRANCH_ID", $"REGION", $"LAST_AUDIT_DATE")

val updatedSummary = branchSummaryReport
  .join(activeBranchDetails, Seq("BRANCH_ID"), "left_outer")
  .withColumn("REGION", when($"REGION".isNotNull, $"REGION").otherwise(lit(null)))
  .withColumn("LAST_AUDIT_DATE", when($"LAST_AUDIT_DATE".isNotNull, $"LAST_AUDIT_DATE").otherwise(lit(null)))
```

## Data Model Updates
### Source Table
| Column Name      | Data Type         | Description                       |
|------------------|------------------|-----------------------------------|
| BRANCH_ID        | INT              | Unique branch identifier          |
| REGION           | VARCHAR2(50)     | Branch region                     |
| MANAGER_NAME     | VARCHAR2(100)    | Branch manager's name             |
| LAST_AUDIT_DATE  | DATE             | Date of last branch audit         |
| IS_ACTIVE        | CHAR(1)          | 'Y' if branch is active, else 'N' |

### Target Table
| Column Name      | Data Type         | Description                       |
|------------------|------------------|-----------------------------------|
| REGION           | STRING           | Branch region                     |
| LAST_AUDIT_DATE  | DATE             | Date of last branch audit         |

## Source-to-Target Mapping
| Source Table                | Source Column      | Target Table           | Target Column      | Transformation Rule                                                                 |
|-----------------------------|-------------------|------------------------|-------------------|-------------------------------------------------------------------------------------|
| BRANCH_OPERATIONAL_DETAILS  | REGION            | BRANCH_SUMMARY_REPORT  | REGION            | If IS_ACTIVE = 'Y', set REGION; else NULL                                           |
| BRANCH_OPERATIONAL_DETAILS  | LAST_AUDIT_DATE   | BRANCH_SUMMARY_REPORT  | LAST_AUDIT_DATE   | If IS_ACTIVE = 'Y', set LAST_AUDIT_DATE; else NULL                                  |

## Assumptions and Constraints
- Full reload of `BRANCH_SUMMARY_REPORT` is required upon deployment.
- Backward compatibility with older records is maintained.
- All transformation logic must be auditable and maintainable.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT

=============================================