from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit
from pyspark.sql.types import *
import logging

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Sales Data Processing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_sales_data(start_date, end_date):
    """
    Convert Hive stored procedure to PySpark DataFrame operations.
    
    This function processes sales data between specified date ranges,
    creates summary tables, and populates detailed sales summary.
    
    Args:
        start_date (str): Start date in string format (e.g., '2023-01-01')
        end_date (str): End date in string format (e.g., '2023-12-31')
    
    Returns:
        bool: True if processing completed successfully, False otherwise
    """
    
    try:
        logger.info(f"Starting sales data processing for period: {start_date} to {end_date}")
        
        # Step 1: Read source sales table
        # Assuming sales_table exists as a Hive table or parquet files
        sales_df = spark.table("sales_table")
        
        # Step 2: Filter data based on date range (equivalent to WHERE clause)
        filtered_sales_df = sales_df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        # Step 3: Create aggregated summary (equivalent to temp_sales_summary)
        # This replaces both the dynamic query and temporary table creation
        temp_sales_summary_df = filtered_sales_df.groupBy("product_id") \
            .agg(spark_sum("sales").alias("total_sales")) \
            .cache()  # Cache for reuse as it's used multiple times
        
        logger.info(f"Created temporary sales summary with {temp_sales_summary_df.count()} records")
        
        # Step 4: Insert into summary_table (equivalent to dynamic query execution)
        # Write the aggregated data to summary_table
        temp_sales_summary_df.write \
            .mode("append") \
            .insertInto("summary_table")
        
        logger.info("Successfully inserted data into summary_table")
        
        # Step 5: Process detailed sales summary (equivalent to cursor operations)
        # Instead of cursor-based row-by-row processing, use DataFrame operations
        # This is more efficient in Spark's distributed environment
        
        # Prepare data for detailed_sales_summary table
        detailed_summary_df = temp_sales_summary_df.select(
            col("product_id"),
            col("total_sales")
        ).filter(col("total_sales").isNotNull())  # Equivalent to WHILE total_sales IS NOT NULL
        
        # Step 6: Insert into detailed_sales_summary table
        # This replaces the cursor-based INSERT operations
        detailed_summary_df.write \
            .mode("append") \
            .insertInto("detailed_sales_summary")
        
        logger.info("Successfully inserted data into detailed_sales_summary")
        
        # Step 7: Cleanup - unpersist cached DataFrame (equivalent to DROP TABLE)
        temp_sales_summary_df.unpersist()
        
        logger.info("Sales data processing completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error during sales data processing: {str(e)}")
        return False

def main():
    """
    Main function to demonstrate usage of the converted stored procedure.
    """
    
    # Example usage with sample date parameters
    start_date = "2023-01-01"
    end_date = "2023-12-31"
    
    # Call the converted stored procedure function
    success = process_sales_data(start_date, end_date)
    
    if success:
        print("Sales data processing completed successfully")
    else:
        print("Sales data processing failed")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()

# Additional utility functions for enhanced functionality

def validate_date_format(date_string):
    """
    Validate date format before processing.
    
    Args:
        date_string (str): Date string to validate
    
    Returns:
        bool: True if valid date format, False otherwise
    """
    from datetime import datetime
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def get_sales_summary_stats(start_date, end_date):
    """
    Get summary statistics for the processed sales data.
    
    Args:
        start_date (str): Start date for analysis
        end_date (str): End date for analysis
    
    Returns:
        dict: Dictionary containing summary statistics
    """
    
    try:
        # Read the summary table
        summary_df = spark.table("summary_table")
        
        # Calculate statistics
        stats = summary_df.agg(
            spark_sum("total_sales").alias("grand_total"),
            col("total_sales").alias("max_sales"),
            col("total_sales").alias("min_sales"),
            col("total_sales").alias("avg_sales")
        ).collect()[0]
        
        return {
            "grand_total": stats["grand_total"],
            "max_sales": stats["max_sales"],
            "min_sales": stats["min_sales"],
            "avg_sales": stats["avg_sales"],
            "total_products": summary_df.count()
        }
        
    except Exception as e:
        logger.error(f"Error calculating summary statistics: {str(e)}")
        return None

# Configuration settings for optimization
SPARK_CONFIGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.execution.arrow.pyspark.enabled": "true"
}

"""
KEY CONVERSION NOTES:

1. STORED PROCEDURE → Python Function:
   - Converted the Hive stored procedure to a Python function with parameters
   - Added proper error handling and logging

2. DYNAMIC SQL → DataFrame Operations:
   - Replaced CONCAT and EXECUTE IMMEDIATE with direct DataFrame transformations
   - Used filter() and groupBy() operations instead of dynamic SQL generation

3. TEMPORARY TABLE → Cached DataFrame:
   - Converted CREATE TEMPORARY TABLE to a cached DataFrame
   - Used cache() for performance optimization in distributed environment

4. CURSOR Operations → DataFrame Transformations:
   - Eliminated row-by-row cursor processing (anti-pattern in Spark)
   - Replaced with bulk DataFrame operations for better performance
   - Used filter() with isNotNull() to handle the WHILE condition

5. INSERT Operations → DataFrame Writes:
   - Converted INSERT statements to DataFrame write operations
   - Used insertInto() method for writing to existing tables
   - Added mode("append") for proper data insertion

6. VARIABLE DECLARATIONS → Function Parameters:
   - Converted DECLARE statements to function parameters and local variables
   - Used proper Python variable naming conventions

7. OPTIMIZATION FEATURES:
   - Added Spark configuration for adaptive query execution
   - Implemented caching strategy for reused DataFrames
   - Added logging for monitoring and debugging
   - Included utility functions for enhanced functionality

8. ERROR HANDLING:
   - Added comprehensive try-catch blocks
   - Implemented proper logging for troubleshooting
   - Added return values to indicate success/failure

This conversion maintains the original logic while leveraging Spark's distributed
computing capabilities for better performance and scalability.
"""