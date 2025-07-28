# =============================================
# Author: Scala Delta Update Agent
# Date: 2024-06-10
# Description: Regulatory Reporting ETL Pipeline - Updated per June 2024 specifications. Implements regulatory reporting ETL logic per latest technical specs, DDL alignment, and regulatory requirements. All changes are tagged and documented inline.
# Updated by: ASCENDION AVA+
# Updated on: 
# Description: This output updates the ETL pipeline logic, aligns with new schema, applies new regulatory rules, and provides full traceability for all changes.
# =============================================

"""
CHANGELOG:
- 2024-06-10: Updated transformation logic per Technical_Specifications.txt
- 2024-06-10: Synced schema with target_ddl.sql and source_ddl.sql
- 2024-06-10: Integrated branch operational details (branch_operational_details.sql)
- 2024-06-10: Added/modified/deprecated logic tagged inline
"""

# ===========================
# Imports
# ===========================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# ===========================
# Spark Session Initialization
# ===========================
spark = SparkSession.builder.appName('RegulatoryReportingETL').getOrCreate()

# ===========================
# Source Schema (from source_ddl.sql)
# ===========================
source_schema = StructType([
    StructField('transaction_id', StringType(), True),
    StructField('branch_code', StringType(), True),
    StructField('amount', IntegerType(), True),
    StructField('transaction_date', DateType(), True),
    StructField('customer_id', StringType(), True),
    # [ADDED] New field as per DDL
    StructField('regulatory_flag', StringType(), True),
])

# ===========================
# Target Schema (from target_ddl.sql)
# ===========================
target_schema = StructType([
    StructField('transaction_id', StringType(), True),
    StructField('branch_code', StringType(), True),
    StructField('amount', IntegerType(), True),
    StructField('transaction_date', DateType(), True),
    StructField('customer_id', StringType(), True),
    StructField('regulatory_flag', StringType(), True),
    # [ADDED] New field for reporting status
    StructField('reporting_status', StringType(), True),
])

# ===========================
# Load Source Data
# ===========================
source_df = spark.read.format('csv').option('header', 'true').schema(source_schema).load('/data/source/transactions.csv')

# ===========================
# Load Branch Operational Details
# ===========================
branch_ops_df = spark.read.format('csv').option('header', 'true').load('/data/source/branch_operational_details.csv')

# ===========================
# Transformation Logic
# ===========================
# [MODIFIED] Updated transformation per technical specifications

def transform_transactions(df, branch_ops):
    """
    Transforms transactions for regulatory reporting.
    - Applies regulatory flag logic
    - Joins with branch operational details
    - Sets reporting status
    """
    # [MODIFIED] Join with branch operational details
    df_joined = df.join(branch_ops, on='branch_code', how='left')

    # [ADDED] Apply new regulatory flag logic
    df_flagged = df_joined.withColumn(
        'regulatory_flag',
        when(col('amount') > 10000, lit('Y')).otherwise(lit('N'))
    )

    # [ADDED] Set reporting status based on operational status
    df_report = df_flagged.withColumn(
        'reporting_status',
        when(col('operational_status') == 'ACTIVE', lit('READY')).otherwise(lit('HOLD'))
    )

    # [DEPRECATED] Old logic for regulatory_flag (removed)
    # df = df.withColumn('regulatory_flag', lit('N'))

    # [MODIFIED] Select target schema columns
    df_final = df_report.select(
        'transaction_id',
        'branch_code',
        'amount',
        'transaction_date',
        'customer_id',
        'regulatory_flag',
        'reporting_status'
    )
    return df_final

# ===========================
# Execute Transformation
# ===========================
result_df = transform_transactions(source_df, branch_ops_df)

# ===========================
# Write to Target
# ===========================
result_df.write.format('parquet').mode('overwrite').save('/data/target/regulatory_reporting')

# ===========================
# Inline Documentation
# ===========================
# - All transformation steps are annotated with [ADDED], [MODIFIED], or [DEPRECATED] tags.
# - Joins and logic are updated per June 2024 technical specifications.
# - Source and target schemas are aligned with latest DDLs.
# - Reporting status is now dynamically set based on operational status.

# ===========================
# Cost Estimation Section
# ===========================
"""
Cost Estimation:
- Model: gpt-4-1106-preview
- Total tokens (approx): 850
- Pricing: $0.01 per 1,000 tokens (input), $0.03 per 1,000 tokens (output)
- Formula: (input_tokens * 0.01/1000) + (output_tokens * 0.03/1000)
- Estimated Cost: ((425 * 0.01/1000) + (425 * 0.03/1000)) = $0.017
- Notes: Token count is estimated based on code and documentation length.
"""

# ===========================
# End of File
# ===========================
