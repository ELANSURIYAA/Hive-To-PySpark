from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit
from pyspark.sql.types import *
import logging

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ProcessSalesData") \
    .enableHiveSupport() \
    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_sales_data(start_date, end_date):
    """
    Process sales data for a given date range.
    
    This function replicates the logic of the Hive stored procedure:
    1. Filters sales data by date range
    2. Aggregates sales by product_id
    3. Inserts results into summary tables
    
    Args:
        start_date (str): Start date in string format (e.g., '2023-01-01')
        end_date (str): End date in string format (e.g., '2023-12-31')
    
    Returns:
        None: Results are written to target tables
    """
    
    try:
        logger.info(f"Starting sales data processing for date range: {start_date} to {end_date}")
        
        # Read source sales table
        # Replace 'sales_table' with your actual table name/path
        sales_df = spark.table("sales_table")
        
        # Filter sales data by date range (equivalent to WHERE clause in original procedure)
        filtered_sales_df = sales_df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        # Aggregate sales by product_id (equivalent to GROUP BY in original procedure)
        # This replaces both the dynamic query and temp table creation
        sales_summary_df = filtered_sales_df.groupBy("product_id") \
            .agg(spark_sum("sales").alias("total_sales"))
        
        # Cache the aggregated data for reuse (optimization for multiple operations)
        sales_summary_df.cache()
        
        logger.info(f"Processed {sales_summary_df.count()} product records")
        
        # Insert into summary_table (equivalent to first INSERT in original procedure)
        # Mode 'overwrite' can be changed to 'append' based on requirements
        sales_summary_df.write \
            .mode("append") \
            .insertInto("summary_table")
        
        logger.info("Successfully inserted data into summary_table")
        
        # Process detailed sales summary (equivalent to cursor operations in original procedure)
        # Instead of row-by-row cursor processing, we use DataFrame operations
        # This is more efficient in Spark's distributed environment
        
        # Read existing detailed_sales_summary table to understand schema
        # If table doesn't exist, create it with proper schema
        try:
            existing_detailed_df = spark.table("detailed_sales_summary")
            logger.info("Found existing detailed_sales_summary table")
        except Exception as e:
            logger.info("Creating detailed_sales_summary table schema")
            # Define schema for detailed_sales_summary table
            detailed_schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("total_sales", FloatType(), True)
            ])
        
        # Insert into detailed_sales_summary (equivalent to cursor INSERT operations)
        # This replaces the cursor-based row-by-row processing with bulk DataFrame operation
        sales_summary_df.write \
            .mode("append") \
            .insertInto("detailed_sales_summary")
        
        logger.info("Successfully inserted data into detailed_sales_summary")
        
        # Unpersist cached DataFrame to free memory
        sales_summary_df.unpersist()
        
        logger.info("Sales data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing sales data: {str(e)}")
        raise e

def main():
    """
    Main function to demonstrate usage of process_sales_data function.
    Replace the date parameters with actual values as needed.
    """
    
    # Example usage - replace with actual date parameters
    start_date = "2023-01-01"
    end_date = "2023-12-31"
    
    # Call the main processing function
    process_sales_data(start_date, end_date)
    
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
        bool: True if valid, False otherwise
    """
    from datetime import datetime
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def get_sales_statistics(start_date, end_date):
    """
    Get additional statistics for the processed sales data.
    
    Args:
        start_date (str): Start date
        end_date (str): End date
    
    Returns:
        dict: Dictionary containing statistics
    """
    
    sales_df = spark.table("sales_table")
    
    filtered_sales_df = sales_df.filter(
        (col("sale_date") >= lit(start_date)) & 
        (col("sale_date") <= lit(end_date))
    )
    
    stats = {
        'total_records': filtered_sales_df.count(),
        'unique_products': filtered_sales_df.select("product_id").distinct().count(),
        'total_sales_amount': filtered_sales_df.agg(spark_sum("sales")).collect()[0][0]
    }
    
    return stats

# Configuration settings
SPARK_CONFIG = {
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    'spark.sql.adaptive.skewJoin.enabled': 'true',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
}

# Apply configuration for optimized performance
for key, value in SPARK_CONFIG.items():
    spark.conf.set(key, value)