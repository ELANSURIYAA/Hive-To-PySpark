| Component Name | Component Type | Input Ports | Output Ports | Expressions/Logic | Dependencies/Connections | Additional Properties |
|----------------|---------------|------------|-------------|-------------------|--------------------------|-----------------------|
| lkp_dynamic_DIM_MEMBER | Lookup (Dynamic) | i_MEMBER_NBR, i_MEMBER_DIM_CK, i_PLAN_DIM_CK, i_VERSION_EFFECTIVE_DATE, i_EDW_MEMBER_CK | MEMBER_DIM_CK, MEMBER_NBR, PLAN_DIM_CK, EDW_MEMBER_CK, VERSION_EFFECTIVE_DATE, NewLookupRow | Dynamic lookup on DIM_MEMBER, join on MEMBER_NBR and PLAN_DIM_CK | exp_TRIM_DATA, exp_FIND_EDW_MEMBER_CK, seq_DIM_MEMBER, mplt_COMMON_RETRIEVE_DIM_PLAN_CK | Dynamic cache enabled, ODBC
| lkp_MEMBER_SPAN | Lookup | i_MEMBER_NBR, i_BUS_UNIT | HEALTHSTAT, MARITALSTAT, RISKPOP, STATUS_X, REGION, BENEFIT_PKG, CARRIER, MEMBER_NBR, BUS_UNIT | Lookup on MEMBER_SPAN | sq_MEMBER | Lookup caching enabled, ODBC
| lkp_ALT_MEMBER_INFO_TXFC | Lookup | i_MEMBER_NBR, i_BUS_UNIT | MEMBER_NBR, DFPS_ID, ETHNICITY, RACE, CERT_DATE, ALOC_CODE, ALOC_START_DATE, ALOC_END_DATE, FORENSIC_IND, LAST_ACTION_DATE, BUS_UNIT | Lookup on ALT_MEMBER_INFO_TXFC | sq_MEMBER | Lookup caching enabled, ODBC
| lkp_ALT_IDENTIFIER | Lookup | i_MEMBER_NBR, i_BUS_UNIT | MEMBER_NBR, ALT_ID_WHO, BUS_UNIT | Lookup on ALT_IDENTIFIER | exp_LTRIM_RTRIM | Lookup caching enabled, ODBC
| lkp_HNT_PERSONID_MBRTYPE_XWALK | Lookup | i_ALT_ID_WHO_LAST_2_DIGIT | PERSON_CODE, MEMBER_TYPE | Lookup on HNT_PERSONID_MBRTYPE_XWALK | exp_ALT_ID_WHO | Lookup caching enabled, ODBC
| lkp_DIM_RACE | Lookup | PLAN_DIM_CK_in, RACE_in | RACE_DIM_CK, RACE_CODE, RACE_DESC, SOURCE_DIM_CK, DELETED_IND, PLAN_DIM_CK | Lookup on DIM_RACE | exp_RACE_CODE | Lookup caching enabled, ODBC
| lkp_DIM_RACE_by_desc | Lookup | PLAN_DIM_CK_in, RACE_in | RACE_DIM_CK, RACE_CODE, RACE_DESC, SOURCE_DIM_CK, DELETED_IND, PLAN_DIM_CK | Lookup on DIM_RACE by description | exp_RACE_CODE | Lookup caching enabled, ODBC
| rtr_DIM_MEMBER_NEW | Router | INPUT | DEFAULT1, NEW_ROW_MEMBER | Group filter: NEW_ROW_MEMBER = NewLookupRow = 1 | _EXPR_rtr_DIM_MEMBER_NEW | Tracing Level: Normal, Optional: true
| upd_VERSION_END | Transformation (Expression/Update Strategy) | DEFAULT1, exp_AUDIT_FIELDS11 | Update_Strategy_Expression_48375 | Update logic for version end | rtr_DIM_MEMBER_NEW, exp_AUDIT_FIELDS11 | N/A
| DIM_MEMBER_UPDATE_VERSION_END | Target | DefaultGroup | N/A | Upsert to DIM_MEMBER_UPDATE_VERSION_END | upd_VERSION_END, _EXPR_DIM_MEMBER_UPDATE_VERSION_END | Teradata, ODBC, Parameterized target
| DIM_MEMBER | Target | DefaultGroup | N/A | Insert to DIM_MEMBER | _EXPR_DIM_MEMBER | Teradata, ODBC, Parameterized target
| DIM_MEMBER_VERSION_INSERT | Target | DefaultGroup | N/A | Insert to DIM_MEMBER_VERSION_INSERT | _EXPR_DIM_MEMBER_VERSION_INSERT | Teradata, ODBC, Parameterized target
| DIM_MEMBER_MASTER | Target | DefaultGroup | N/A | Insert to DIM_MEMBER_MASTER | _EXPR_DIM_MEMBER_MASTER | Teradata, ODBC, Parameterized target
