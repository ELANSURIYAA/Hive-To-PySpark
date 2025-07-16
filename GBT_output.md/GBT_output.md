# Informatica Pipeline Metadata Extraction

## Component Metadata Table

| Component Name                | Component Type   | Input Ports/Fields                                      | Output Ports/Fields                                     | Expressions/Logic (truncated)                   | Dependencies/Connections                          | Additional Properties                                   |
|------------------------------|------------------|---------------------------------------------------------|---------------------------------------------------------|--------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| sq_MEMBER                    | Source           | N/A                                                    | STAGING_CK1, MEMBER_NBR1, CONTRACT_NBR, ...            | N/A                                             | N/A                                                   | Database: $Param_Source_Owner_EV                        |
| exp_TRIM_DATA                | Transformation   | All fields from sq_MEMBER and other upstreams           | o_BUS_UNIT, o_MEMBER_NBR, o_CONTRACT_NBR, ...           | Trimming, null checks, business rules...         | sq_MEMBER, lkp_MEMBER_SPAN, lkp_ALT_MEMBER_INFO_TXFC   | Expression: see below                                   |
| lkp_DIM_MARITAL_STATUS       | Lookup           | o_MARITALSTAT, PLAN_DIM_CK1                             | MARITAL_STATUS_DIM_CK, MARITAL_STATUS_CODE, PLAN_DIM_CK | Lookup on Marital Status table                   | exp_TRIM_DATA, mplt_COMMON_RETRIEVE_DIM_PLAN_CK        | Cache: Enabled                                         |
| mplt_MARITAL_STATUS_DEFAULT_KEYS | Mapplet     | o_MARITALSTAT, PLAN_DIM_CK1                             | PARENT_DIM_CK                                           | Default key logic                                 | lkp_DIM_MARITAL_STATUS, exp_TRIM_DATA                  | Mapplet: Default Key                                    |
| lkp_DIM_MEDICAID_STATUS      | Lookup           | o_STATUS_X, PLAN_DIM_CK1                                | MEDICAID_STATUS_DIMENSION_CK, ...                       | Lookup on Medicaid Status table                  | exp_TRIM_DATA, mplt_COMMON_RETRIEVE_DIM_PLAN_CK        | Cache: Enabled                                         |
| mplt_MEDICAID_STATUS_SET_DEFAULT_KEYS | Mapplet | o_STATUS_X, PLAN_DIM_CK1                                | PARENT_DIM_CK                                           | Default key logic                                 | lkp_DIM_MEDICAID_STATUS, exp_TRIM_DATA                 | Mapplet: Default Key                                    |
| lkp_DIM_AUTHORIZED_LOC       | Lookup           | o_ALOC_CODE                                            | AUTHORIZED_LOC_DIM_CK, AUTHORIZED_LOC_CODE              | Lookup on Authorized Location table              | exp_TRIM_DATA                                         | Cache: Enabled                                         |
| mplt_AUTHORIZED_LOC_CK_SET_CK_ERROR | Mapplet | o_ALOC_CODE                                            | PARENT_DIM_CK                                           | Default key logic                                 | lkp_DIM_AUTHORIZED_LOC, exp_TRIM_DATA                  | Mapplet: Default Key                                    |
| exp_ADDRESS_VALIDATE         | Transformation   | i_CONTRACT_NBR, i_SOURCE_INSTANCE_ID, ADDR_WHO, ...    | o_ADDRESS1, o_ADDRESS2, o_CITY, o_COUNTRY, ...          | Trimming, null checks, address validation...      | lkp_AMYSIS_ADDRESS, mplt_COMMON_RETRIEVE_DIM_SOURCE_BY_CODE_CK | Expression: see below                        |
| lkp_DIM_ADDRESS              | Lookup           | o_ADDRESS1, o_ADDRESS2, o_CITY, o_COUNTRY, ...         | ADDRESS_DIM_CK, SOURCE_DIM_CK, ...                      | Lookup on Address table                          | exp_ADDRESS_VALIDATE                                  | Cache: Enabled                                         |
| mplt_DIM_ADDRESS_SET_DEFAULT_KEYS | Mapplet    | o_ADDRESS1, o_ADDRESS2, o_CITY, o_COUNTRY, ...         | PARENT_DIM_CK                                           | Default key logic                                 | lkp_DIM_ADDRESS, exp_ADDRESS_VALIDATE                  | Mapplet: Default Key                                    |
| exp_AUDIT_FIELDS1            | Transformation   | o_LAST_ACTION_USR_rtr_DIM_MEMBER_NEW, ...              | DELETED_IND, VERSION_END_DATE, ACTIVE_IND, ...          | Audit fields, status logic...                    | rtr_DIM_MEMBER_NEW                                    | Expression: see below                                   |
| exp_AUDIT_FIELDS11           | Transformation   | o_LAST_ACTION_USR_rtr_DIM_MEMBER_NEW, ...              | DELETED_IND, VERSION_END_DATE, ACTIVE_IND, ...          | Audit fields, status logic...                    | rtr_DIM_MEMBER_NEW                                    | Expression: see below                                   |
| rtr_DIM_MEMBER_NEW           | Router           | All fields from upstream                                | DEFAULT1, NEW_ROW_MEMBER                               | Routing based on new/updated member             | _EXPR_rtr_DIM_MEMBER_NEW                               | Group filter: NewLookupRow = 1                          |
| upd_VERSION_END              | Transformation   | DEFAULT1, exp_AUDIT_FIELDS11                            | DefaultGroup                                            | Update strategy logic                            | rtr_DIM_MEMBER_NEW, exp_AUDIT_FIELDS11                 | Expression: see below                                   |
| DIM_MEMBER_UPDATE_VERSION_END | Target          | DefaultGroup                                            | N/A                                                     | N/A                                             | upd_VERSION_END, _EXPR_DIM_MEMBER_UPDATE_VERSION_END    | Target: $Param_Target_Owner                              |
| DIM_MEMBER                   | Target           | DefaultGroup                                            | N/A                                                     | N/A                                             | _EXPR_DIM_MEMBER                                       | Target: $Param_Target_Owner                              |
| DIM_MEMBER_VERSION_INSERT    | Target           | DefaultGroup                                            | N/A                                                     | N/A                                             | _EXPR_DIM_MEMBER_VERSION_INSERT                         | Target: $Param_Target_Owner                              |
| DIM_MEMBER_MASTER            | Target           | DefaultGroup                                            | N/A                                                     | N/A                                             | _EXPR_DIM_MEMBER_MASTER                                 | Target: $Param_Target_Owner                              |
| ...                          | ...              | ...                                                     | ...                                                     | ...                                              | ...                                                    | ...                                                    |

## Notes
- This table is a partial extract due to the very large number of components and fields. All major sources, lookups, transformations, maplets, routers, and targets are included.
- For each transformation, the 'Expressions/Logic' column is truncated with ellipsis (`...`). Full expressions are available in the JSON and can be detailed on request.
- Connections/dependencies are derived from the mapping links and group rules.
- Additional properties include database/schema, cache settings, and mapplet/target details.
- Placeholders like "N/A" are used where a property is not applicable or not present.
- Some helper/utility expressions and maplets are omitted for brevity but are present in the mapping.

---

### Example: Expression Logic (Truncated)

#### exp_TRIM_DATA (selected output fields):
- o_BUS_UNIT: `IIF( Length(LTRIM(RTRIM(BUS_UNIT))) = 0,  NULL, LTRIM(RTRIM(BUS_UNIT)))`
- o_MEMBER_NBR: `IIF( Length(LTRIM(RTRIM(MEMBER_NBR))) = 0,  NULL, LTRIM(RTRIM(MEMBER_NBR)))`
- o_CONTRACT_NBR: `LTRIM(RTRIM(CONTRACT_NBR))`
- o_SSN: `IIF((v_BUS_UNIT = 'TX' OR v_BUS_UNIT = 'CT'), HCFA_NBR_SSN_TX, ...)`

#### exp_ADDRESS_VALIDATE (selected output fields):
- o_ADDRESS1: `IIF( Length(LTRIM(RTRIM(ADDRESS1))) = 0,  '', LTRIM(RTRIM(ADDRESS1)))`
- o_COUNTRY: `IIF( Length(LTRIM(RTRIM(COUNTRY))) = 0,  'USA', LTRIM(RTRIM(COUNTRY)))`

---

### Unsupported/Missing Properties
- Some advanced session properties, adapter-specific settings, and certain mapplet internals are not shown in this summary table but are available in the full JSON.
- If a field/property is not present in the JSON, it is marked as "N/A".

---

### Guidance
- For a full list of all fields, ports, and expressions for each component, refer to the JSON or request a detailed breakdown of a specific component.
- This markdown table is designed for high-level review, migration planning, and governance.
