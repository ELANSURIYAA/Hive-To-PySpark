# PySpark conversion of Hive Stored Procedure: process_sales_data
# Converted from Hive SQL to PySpark DataFrame API
# Author: Data Engineering Team
# Date: Auto-generated conversion

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import logging

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ProcessSalesData") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_sales_data(start_date: str, end_date: str):
    """
    PySpark equivalent of Hive stored procedure process_sales_data
    
    Parameters:
    start_date (str): Start date for sales data filtering (format: 'YYYY-MM-DD')
    end_date (str): End date for sales data filtering (format: 'YYYY-MM-DD')
    
    Returns:
    None: Function performs data processing and writes to target tables
    """
    
    try:
        logger.info(f"Starting sales data processing for period: {start_date} to {end_date}")
        
        # Step 1: Read source sales table
        # Assuming sales_table exists as a Hive table or parquet files
        sales_df = spark.table("sales_table")
        
        # Step 2: Filter sales data by date range
        # Equivalent to: WHERE sale_date BETWEEN start_date AND end_date
        filtered_sales_df = sales_df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        # Step 3: Create aggregated sales summary
        # Equivalent to: SELECT product_id, SUM(sales) AS total_sales GROUP BY product_id
        sales_summary_df = filtered_sales_df.groupBy("product_id") \
            .agg(spark_sum("sales").alias("total_sales")) \
            .orderBy("product_id")
        
        # Cache the summary for multiple operations
        sales_summary_df.cache()
        
        logger.info(f"Processed {sales_summary_df.count()} product summaries")
        
        # Step 4: Insert into summary_table (equivalent to dynamic query execution)
        # This replaces the dynamic SQL generation and EXECUTE IMMEDIATE
        sales_summary_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("summary_table")
        
        logger.info("Successfully inserted data into summary_table")
        
        # Step 5: Create temporary view for detailed processing
        # Equivalent to: CREATE TEMPORARY TABLE temp_sales_summary
        sales_summary_df.createOrReplaceTempView("temp_sales_summary")
        
        # Step 6: Process each row for detailed summary (replaces cursor operations)
        # In PySpark, we avoid row-by-row processing and use DataFrame operations instead
        # This is more efficient than the original cursor-based approach
        
        # Read existing detailed_sales_summary table structure or create if not exists
        try:
            existing_detailed_df = spark.table("detailed_sales_summary")
            logger.info("Found existing detailed_sales_summary table")
        except Exception:
            # Create table structure if it doesn't exist
            logger.info("Creating detailed_sales_summary table structure")
            
        # Step 7: Insert all records into detailed_sales_summary in batch
        # This replaces the cursor-based row-by-row insertion
        # Much more efficient than the original WHILE loop approach
        detailed_summary_df = sales_summary_df.select(
            col("product_id"),
            col("total_sales")
        )
        
        # Write to detailed_sales_summary table
        detailed_summary_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("detailed_sales_summary")
        
        logger.info("Successfully inserted data into detailed_sales_summary")
        
        # Step 8: Cleanup - unpersist cached DataFrame
        # Equivalent to: DROP TABLE temp_sales_summary
        sales_summary_df.unpersist()
        spark.catalog.dropTempView("temp_sales_summary")
        
        logger.info("Sales data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in process_sales_data: {str(e)}")
        raise e

def process_sales_data_with_validation(start_date: str, end_date: str):
    """
    Enhanced version with data validation and error handling
    """
    
    # Input validation
    if not start_date or not end_date:
        raise ValueError("Start date and end date must be provided")
    
    if start_date > end_date:
        raise ValueError("Start date must be less than or equal to end date")
    
    # Execute main processing
    process_sales_data(start_date, end_date)

# Alternative implementation using SQL for teams preferring SQL syntax
def process_sales_data_sql_approach(start_date: str, end_date: str):
    """
    Alternative implementation using Spark SQL for teams more comfortable with SQL
    """
    
    try:
        logger.info(f"Starting SQL-based sales data processing for period: {start_date} to {end_date}")
        
        # Create summary and insert into summary_table
        spark.sql(f"""
            INSERT INTO summary_table
            SELECT product_id, SUM(sales) AS total_sales
            FROM sales_table 
            WHERE sale_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY product_id
        """)
        
        # Create temporary view
        spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW temp_sales_summary AS
            SELECT product_id, SUM(sales) AS total_sales
            FROM sales_table
            WHERE sale_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY product_id
        """)
        
        # Insert into detailed summary (batch operation instead of cursor)
        spark.sql("""
            INSERT INTO detailed_sales_summary (product_id, total_sales)
            SELECT product_id, total_sales
            FROM temp_sales_summary
        """)
        
        # Cleanup
        spark.sql("DROP VIEW IF EXISTS temp_sales_summary")
        
        logger.info("SQL-based sales data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in SQL-based process_sales_data: {str(e)}")
        raise e

# Main execution function
if __name__ == "__main__":
    # Example usage
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    
    # Choose implementation approach
    # Option 1: DataFrame API approach (recommended for better performance)
    process_sales_data_with_validation(start_date, end_date)
    
    # Option 2: SQL approach (uncomment if preferred)
    # process_sales_data_sql_approach(start_date, end_date)
    
    # Stop Spark session
    spark.stop()

# Performance optimization notes:
# 1. Replaced cursor-based row-by-row processing with batch DataFrame operations
# 2. Added caching for DataFrames used multiple times
# 3. Used adaptive query execution for better performance
# 4. Implemented proper error handling and logging
# 5. Added input validation
# 6. Provided both DataFrame API and SQL approaches for flexibility

# Key transformations applied:
# - Dynamic SQL -> Static DataFrame operations with parameters
# - CURSOR operations -> Batch DataFrame transformations
# - Row-by-row INSERT -> Bulk DataFrame write operations
# - Temporary tables -> Temporary views with proper cleanup
# - Procedural logic -> Functional programming approach