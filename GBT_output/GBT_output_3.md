| Component Name | Component Type | Input Ports | Output Ports | Expressions/Logic | Dependencies/Connections | Additional Properties |
|----------------|---------------|------------|-------------|-------------------|--------------------------|-----------------------|
| mplt_COMMON_RETRIEVE_DIM_PLAN_CK | Mapplet | BUSINESS_UNIT, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | PLAN_DIM_CK | Retrieves PLAN_DIM_CK based on business unit and null indicators | Upstream: exp_TRIM_DATA | Mapplet, Parameterized Connection |
| mplt_COMMON_RETRIEVE_DIM_SOURCE_BY_CODE_CK | Mapplet | SOURCE_SYSTEM_CODE, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | PARENT_DIM_CK | Retrieves PARENT_DIM_CK (source dimension key) | Upstream: exp_TRIM_DATA | Mapplet, Parameterized Connection |
| mplt_COMMON_RETRIEVE_DIM_BUS_LINE_CK | Mapplet | BUS_UNIT | BUS_LINE_DIM_CK | Retrieves BUS_LINE_DIM_CK (bus line dimension key) | Upstream: exp_TRIM_DATA | Mapplet, Parameterized Connection |
| mplt_DIM_RISK_POP_SET_DEFAULT_KEYS | Mapplet | PARENT_DIM_CK, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | PARENT_DIM_CK | Sets default keys for risk population dimension | Upstream: exp_TRIM_DATA, lkp_DIM_RISK_POPULATION | Mapplet |
| mplt_RACE_SET_DEFAULT_KEYS | Mapplet | PARENT_DIM_CK, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | PARENT_DIM_CK | Sets default keys for race dimension | Upstream: exp_RACE_CODE | Mapplet |
| mplt_AUTHORIZED_LOC_CK_SET_CK_ERROR | Mapplet | PARENT_DIM_CK, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | PARENT_DIM_CK | Sets default keys for authorized location dimension | Upstream: exp_TRIM_DATA, lkp_DIM_AUTHORIZED_LOC | Mapplet |
| mplt_DIM_ADDRESS_SET_DEFAULT_KEYS | Mapplet | PARENT_DIM_CK, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | PARENT_DIM_CK | Sets default keys for address dimension | Upstream: exp_ADDRESS_VALIDATE, lkp_DIM_ADDRESS | Mapplet |
| mplt_DIM_ETHINIC_GROUP_SET_DEFAULT_KEYS | Mapplet | PARENT_DIM_CK, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | PARENT_DIM_CK | Sets default keys for ethnic group dimension | Upstream: exp_TRIM_DATA, lkp_DIM_ETHNIC_GROUP | Mapplet |
| mplt_COMMON_SET_DEFAULT_KEYS1 | Mapplet | PARENT_DIM_CK, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | PARENT_DIM_CK | Sets default keys for common dimension | Upstream: exp_TRIM_DATA | Mapplet |
| mplt_DEATH_DATE_DIM_CK | Mapplet | DATE, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | DATE_DIM_CK | Retrieves DATE_DIM_CK for death date | Upstream: exp_TRIM_DATA | Mapplet |
| mplt_ALOC_START_DIM_DATE_CK | Mapplet | DATE, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | DATE_DIM_CK | Retrieves DATE_DIM_CK for allocation start date | Upstream: exp_TRIM_DATA | Mapplet |
| mplt_BIRTH_DATE_DIM_CK | Mapplet | DATE, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | DATE_DIM_CK | Retrieves DATE_DIM_CK for birth date | Upstream: exp_TRIM_DATA | Mapplet |
| mplt_CARD_DIM_DATE_CK | Mapplet | DATE, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | DATE_DIM_CK | Retrieves DATE_DIM_CK for card date | Upstream: exp_TRIM_DATA | Mapplet |
| mplt_ALOC_END_DIM_DATE_CK | Mapplet | DATE, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | DATE_DIM_CK | Retrieves DATE_DIM_CK for allocation end date | Upstream: exp_TRIM_DATA | Mapplet |
| mplt_CERT_DATE_CK | Mapplet | DATE, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | DATE_DIM_CK | Retrieves DATE_DIM_CK for certification date | Upstream: exp_TRIM_DATA | Mapplet |
| mplt_RETRIEVE_DIM_REGION_CK | Mapplet | REGION, PLAN_DIM_ID, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | REGION_DIM_CK | Retrieves REGION_DIM_CK | Upstream: exp_TRIM_DATA | Mapplet |
| mplt_MEDICAID_STATUS_SET_DEFAULT_KEYS | Mapplet | PARENT_DIM_CK, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | PARENT_DIM_CK | Sets default keys for medicaid status dimension | Upstream: exp_TRIM_DATA, lkp_DIM_MEDICAID_STATUS | Mapplet |
| mplt_MARITAL_STATUS_DEFAULT_KEYS | Mapplet | PARENT_DIM_CK, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | PARENT_DIM_CK | Sets default keys for marital status dimension | Upstream: exp_TRIM_DATA, lkp_DIM_MARITAL_STATUS | Mapplet |
| mplt_SET_HEALTH_STATUS_DEFAULT_KEYS | Mapplet | PARENT_DIM_CK, NATURAL_KEY_NULL_IND, NULL_ALLOWED_IND | PARENT_DIM_CK | Sets default keys for health status dimension | Upstream: exp_TRIM_DATA, lkp_DIM_HEALTH_STATUS | Mapplet |
| seq_DIM_MEMBER | Sequence Generator | N/A | NEXTVAL | Generates sequence for MEMBER_DIM_CK | Upstream: exp_TO_BIGINT_SEQ | Shared Sequence: @77pFN69odWchLnyOhrNCG0 |
| seq_EDW_MEMBER_CK | Sequence Generator | N/A | NEXTVAL | Generates sequence for EDW_MEMBER_CK | Upstream: exp_FIND_EDW_MEMBER_CK | Shared Sequence: @fY1a8uVaWuKjN6q5As7WaT |
| rtr_DIM_MEMBER_NEW | Router | INPUT | DEFAULT1, NEW_ROW_MEMBER | Routes rows based on group filter conditions (e.g., NewLookupRow = 1) | Upstream: _EXPR_rtr_DIM_MEMBER_NEW | Group Filters: NEW_ROW_MEMBER, DEFAULT1 |
