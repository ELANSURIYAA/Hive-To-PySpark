# Informatica Pipeline Component Metadata

| Component Name                | Component Type   | Input Ports / Fields (Sample)        | Output Ports / Fields (Sample)       | Expressions/Logic (Truncated)            | Dependencies/Connections (Sample)            | Additional Properties (Sample)              |
|------------------------------|------------------|--------------------------------------|--------------------------------------|------------------------------------------|--------------------------------------------|---------------------------------------------|
| sq_MEMBER                    | Source           | STAGING_CK, MEMBER_NBR, ...          | STAGING_CK, MEMBER_NBR, ...          | N/A                                      | N/A                                       | Database: $Param_Source_Owner_EV           |
| exp_TRIM_DATA                | Transformation   | BUS_UNIT, MEMBER_NBR, ...            | o_BUS_UNIT, o_MEMBER_NBR, ...        | Trims, cleans, and nullifies fields      | sq_MEMBER                                 | Expression Type: Data Cleansing            |
| lkp_MEMBER_SPAN              | Lookup           | MEMBER_NBR, BUS_UNIT                 | HEALTHSTAT, MARITALSTAT, ...         | Lookup on MEMBER_NBR, BUS_UNIT           | exp_TRIM_DATA                             | Cache: Enabled, Table: MEMBER_SPAN         |
| lkp_ALT_MEMBER_INFO_TXFC     | Lookup           | MEMBER_NBR, BUS_UNIT                 | RACE, ETHNICITY, ...                 | Lookup on MEMBER_NBR, BUS_UNIT           | sq_MEMBER                                 | Table: ALT_MEMBER_INFO_TXFC                |
| _EXPR_exp_TRIM_DATA          | Expression       | BUS_UNIT, LAST_ACTION_DATE, ...       | BUS_UNIT, LAST_ACTION_DATE, ...      | Helper for exp_TRIM_DATA                 | sq_MEMBER, lkp_MEMBER_SPAN, lkp_ALT_MEMBER_INFO_TXFC | Helper Expression                         |
| exp_ADDRESS_VALIDATE         | Transformation   | ADDRESS1, ADDRESS2, CITY, ...        | o_ADDRESS1, o_ADDRESS2, ...          | Trims, null checks, default logic        | lkp_AMYSIS_ADDRESS, mplt_COMMON_RETRIEVE_DIM_SOURCE_BY_CODE_CK | Address Validation Logic                   |
| lkp_AMYSIS_ADDRESS           | Lookup           | ADDR_WHO, SOURCE_INSTANCE_ID          | ADDRESS1, ADDRESS2, ...              | Lookup on ADDR_WHO, SOURCE_INSTANCE_ID   | exp_TRIM_DATA                             | Table: ADDRESS                            |
| lkp_DIM_MARITAL_STATUS       | Lookup           | o_MARITALSTAT, PLAN_DIM_CK1           | MARITAL_STATUS_DIM_CK, ...           | Lookup on MARITAL_STATUS_CODE, PLAN_DIM_CK | exp_TRIM_DATA, mplt_COMMON_RETRIEVE_DIM_PLAN_CK | Table: DIM_MARITAL_STATUS                  |
| mplt_MARITAL_STATUS_DEFAULT_KEYS | Mapplet      | PARENT_DIM_CK, NATURAL_KEY_NULL_IND   | PARENT_DIM_CK                        | Default key logic for marital status     | lkp_DIM_MARITAL_STATUS, exp_TRIM_DATA     | Mapplet                                    |
| lkp_DIM_MEDICAID_STATUS      | Lookup           | o_STATUS_X, PLAN_DIM_CK1              | MEDICAID_STATUS_DIMENSION_CK, ...    | Lookup on MEDICAID_STATUS_CODE, PLAN_DIM_CK | exp_TRIM_DATA, mplt_COMMON_RETRIEVE_DIM_PLAN_CK | Table: DIM_MEDICAID_STATUS                 |
| mplt_MEDICAID_STATUS_SET_DEFAULT_KEYS | Mapplet | PARENT_DIM_CK, NATURAL_KEY_NULL_IND   | PARENT_DIM_CK                        | Default key logic for Medicaid status    | lkp_DIM_MEDICAID_STATUS, exp_TRIM_DATA    | Mapplet                                    |
| lkp_DIM_HEALTH_STATUS        | Lookup           | o_HEALTH_STAT, PLAN_DIM_CK1           | HEALTH_STATUS_DIM_CK, ...            | Lookup on HEALTH_STATUS_CODE, PLAN_DIM_CK | exp_TRIM_DATA, mplt_COMMON_RETRIEVE_DIM_PLAN_CK | Table: DIM_HEALTH_STATUS                   |
| mplt_SET_HEALTH_STATUS_DEFAULT_KEYS | Mapplet   | PARENT_DIM_CK, NATURAL_KEY_NULL_IND   | PARENT_DIM_CK                        | Default key logic for health status      | lkp_DIM_HEALTH_STATUS, exp_TRIM_DATA      | Mapplet                                    |
| lkp_DIM_RISK_POPULATION      | Lookup           | o_RISK_POP, PLAN_DIM_CK1              | RISK_POPULATION_DIM_CK, ...          | Lookup on RISK_POPULATION_CODE, PLAN_DIM_CK | exp_TRIM_DATA, mplt_COMMON_RETRIEVE_DIM_PLAN_CK | Table: DIM_RISK_POPULATION                 |
| mplt_DIM_RISK_POP_SET_DEFAULT_KEYS | Mapplet    | PARENT_DIM_CK, NATURAL_KEY_NULL_IND   | PARENT_DIM_CK                        | Default key logic for risk population    | lkp_DIM_RISK_POPULATION, exp_TRIM_DATA    | Mapplet                                    |
| lkp_dynamic_DIM_MEMBER       | Lookup           | i_MEMBER_NBR, i_MEMBER_DIM_CK, ...    | MEMBER_DIM_CK, MEMBER_NBR, ...       | Dynamic lookup for member changes        | exp_TRIM_DATA, exp_FIND_EDW_MEMBER_CK, seq_DIM_MEMBER | Dynamic Cache                             |
| exp_ADD_TO_DATE_IF_DUP       | Transformation   | VERSION_EFFECTIVE_DATE, ...           | o_VERSION_EFFECTIVE_DATE             | Adds to date if duplicate                | lkp_dynamic_DIM_MEMBER                    | Date Adjustment Logic                       |
| exp_FIND_EDW_MEMBER_CK       | Transformation   | PLAN_DIM_CK, SRC_MBR_ID, ...          | o_EDW_MEMBER_CK                      | Lookup and assign EDW_MEMBER_CK          | seq_EDW_MEMBER_CK, lkp_DIM_PRE_MBR        | Assigns if not found                       |
| exp_RACE_CODE                | Transformation   | RACE, RACE_CODE, PLAN_DIM_CK, ...     | o_RACE_DIM_CK, o_RACE                | Lookup race code/dim CK                  | exp_TRIM_DATA, mplt_COMMON_RETRIEVE_DIM_PLAN_CK | Race Code Logic                            |
| mplt_RACE_SET_DEFAULT_KEYS   | Mapplet          | PARENT_DIM_CK, NATURAL_KEY_NULL_IND   | PARENT_DIM_CK                        | Default key logic for race               | exp_RACE_CODE                             | Mapplet                                    |
| mplt_COMMON_RETRIEVE_DIM_CONTRACT_CK | Mapplet  | CONTRACT_NBR, PLAN_DIM_CK, ...        | CONTRACT_DIM_CK                      | Retrieves contract CK                    | exp_TRIM_DATA, mplt_COMMON_RETRIEVE_DIM_PLAN_CK | Mapplet                                    |
| exp_COMMON_CDC_CHECK         | Transformation   | LAST_ACTION_IND, ...                  | v_LAST_ACTION                        | Validates CDC action flag                | exp_TRIM_DATA                             | CDC Logic                                  |
| lkp_MEMBER_SPAN              | Lookup           | i_MEMBER_NBR, i_BUS_UNIT              | HEALTHSTAT, MARITALSTAT, ...         | Lookup on MEMBER_NBR, BUS_UNIT           | sq_MEMBER                                 | Table: MEMBER_SPAN                         |
| lkp_ALT_MEMBER_INFO_TXFC     | Lookup           | i_MEMBER_NBR, i_BUS_UNIT              | RACE, ETHNICITY, ...                 | Lookup on MEMBER_NBR, BUS_UNIT           | sq_MEMBER                                 | Table: ALT_MEMBER_INFO_TXFC                |
| exp_LTRIM_RTRIM              | Transformation   | MEMBER_NBR, BUS_UNIT, ALT_KEY, ...    | o_MEMBER_NBR, o_BUS_UNIT, ...        | LTRIM/RTRIM for fields                   | sq_MEMBER                                 | Data Cleansing                             |
| lkp_ALT_IDENTIFIER           | Lookup           | i_MEMBER_NBR, i_BUS_UNIT              | ALT_ID_WHO, ...                      | Lookup on MEMBER_NBR, BUS_UNIT           | exp_LTRIM_RTRIM                            | Table: ALT_IDENTIFIER                      |
| exp_ALT_ID_WHO               | Transformation   | i_ALT_ID_WHO, ...                     | o_ALT_ID_WHO, ...                    | Extracts/assigns ALT_ID_WHO              | lkp_ALT_IDENTIFIER, exp_LTRIM_RTRIM        | ALT ID Logic                               |
| lkp_HNT_PERSONID_MBRTYPE_XWALK | Lookup         | i_ALT_ID_WHO_LAST_2_DIGIT             | PERSON_CODE, MEMBER_TYPE             | Lookup on PERSON_CODE                    | exp_ALT_ID_WHO                             | Table: HNT_PERSONID_MBRTYPE_XWALK          |
| exp_MEMBER_TYPE              | Transformation   | i_ALT_ID_WHO, i_MEMBER_TYPE           | o_MEMBER_TYPE                        | Concatenates ALT_ID_WHO and MEMBER_TYPE  | exp_ALT_ID_WHO, lkp_HNT_PERSONID_MBRTYPE_XWALK | Member Type Logic                          |
| seq_DIM_MEMBER               | Sequence         | N/A                                   | NEXTVAL                              | Generates sequence for member             | N/A                                        | Sequence                                   |
| rtr_DIM_MEMBER_NEW           | Router           | INPUT fields                          | DEFAULT1, NEW_ROW_MEMBER             | Routes based on group filter conditions   | _EXPR_rtr_DIM_MEMBER_NEW                   | Group Filters                              |
| exp_AUDIT_FIELDS1            | Transformation   | LAST_ACTION_IND1, ...                 | DELETED_IND, ACTIVE_IND, ...         | Audit fields logic                       | rtr_DIM_MEMBER_NEW                         | Audit Logic                                |
| exp_AUDIT_FIELDS11           | Transformation   | LAST_ACTION_IND1, ...                 | DELETED_IND, ACTIVE_IND, ...         | Audit fields logic                       | rtr_DIM_MEMBER_NEW                         | Audit Logic                                |
| upd_VERSION_END              | Transformation   | DEFAULT1 group fields                 | Update fields                        | Update strategy logic                    | rtr_DIM_MEMBER_NEW, exp_AUDIT_FIELDS11      | Update Strategy                            |
| DIM_MEMBER                   | Target           | i_MEMBER_DIM_CK, i_EDW_MEMBER_CK, ... | N/A                                  | N/A                                      | _EXPR_DIM_MEMBER, rtr_DIM_MEMBER_NEW        | Table: DIM_MEMBER                          |
| DIM_MEMBER_MASTER            | Target           | i_EDW_MEMBER_CK, ...                  | N/A                                  | N/A                                      | _EXPR_DIM_MEMBER_MASTER, rtr_DIM_MEMBER_NEW | Table: DIM_MEMBER_MASTER                   |
| DIM_MEMBER_UPDATE_VERSION_END | Target           | i_MEMBER_DIM_CK2, ...                 | N/A                                  | N/A                                      | _EXPR_DIM_MEMBER_UPDATE_VERSION_END, upd_VERSION_END | Table: DIM_MEMBER_UPDATE_VERSION_END        |
| DIM_MEMBER_VERSION_INSERT    | Target           | i_MEMBER_DIM_CK, ...                  | N/A                                  | N/A                                      | _EXPR_DIM_MEMBER_VERSION_INSERT, rtr_DIM_MEMBER_NEW | Table: DIM_MEMBER_VERSION_INSERT            |


---

## Notes
- **Expressions/Logic** column is truncated for readability. See the JSON for full logic.
- **Dependencies/Connections** column lists only a sample of upstream/downstream links for each component.
- **Additional Properties** column includes key attributes such as database, cache, or transformation type.
- Some mapplets (mplt_*) encapsulate reusable logic for default key assignment or lookups.
- Helper expressions (prefixed with _EXPR_) resolve naming conflicts or support main transformations.
- If any component or property is unsupported or missing, it is marked as "N/A".

---

### Unsupported/Missing Properties
- Some properties, especially for custom or external mapplets, may not be fully parsed from the JSON and are marked as "N/A".
- For very long expressions or SQL overrides, only a summary or the first line is shown in the table. Refer to the JSON for the full logic.

---

### Additional Documentation
- This table is auto-generated from Informatica mapping JSON for the mapping `m_LOAD_DIM_MEMBER_AND_MASTER`.
- For migration, optimization, or compliance, refer to the full JSON for all field-level details and transformation logic.
