from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col, lit
from pyspark.sql.types import *
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
    Convert Hive stored procedure to PySpark DataFrame operations.
    
    This function processes sales data by:
    1. Filtering sales data within the specified date range
    2. Aggregating sales by product_id
    3. Inserting results into summary_table
    4. Creating detailed sales summary records
    
    Args:
        start_date (str): Start date in string format (e.g., '2023-01-01')
        end_date (str): End date in string format (e.g., '2023-12-31')
    
    Returns:
        None: Results are written to target tables
    """
    
    try:
        logger.info(f"Starting sales data processing for date range: {start_date} to {end_date}")
        
        # Step 1: Read sales_table data
        # Assuming sales_table exists as a Hive table or parquet files
        sales_df = spark.table("sales_table")
        
        # Step 2: Filter data by date range and aggregate by product_id
        # This replaces the dynamic query and temp table creation
        filtered_sales_df = sales_df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        # Step 3: Aggregate sales by product_id (equivalent to temp_sales_summary)
        sales_summary_df = filtered_sales_df.groupBy("product_id") \
            .agg(spark_sum("sales").alias("total_sales")) \
            .select("product_id", "total_sales")
        
        # Step 4: Cache the aggregated data for reuse (optimization)
        sales_summary_df.cache()
        
        logger.info(f"Aggregated sales data for {sales_summary_df.count()} products")
        
        # Step 5: Insert aggregated results into summary_table
        # This replaces the dynamic INSERT statement
        sales_summary_df.write \
            .mode("append") \
            .insertInto("summary_table")
        
        logger.info("Successfully inserted data into summary_table")
        
        # Step 6: Process detailed sales summary
        # This replaces the cursor-based iteration
        # Instead of row-by-row processing, we use DataFrame operations
        
        # Read existing detailed_sales_summary table structure
        try:
            existing_detailed_df = spark.table("detailed_sales_summary")
            detailed_schema = existing_detailed_df.schema
        except:
            # If table doesn't exist, create schema
            detailed_schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("total_sales", FloatType(), True)
            ])
        
        # Step 7: Prepare data for detailed_sales_summary insertion
        # Filter out null total_sales (equivalent to cursor condition)
        detailed_sales_df = sales_summary_df.filter(col("total_sales").isNotNull())
        
        # Step 8: Insert into detailed_sales_summary table
        # This replaces the cursor-based INSERT loop
        detailed_sales_df.write \
            .mode("append") \
            .insertInto("detailed_sales_summary")
        
        logger.info(f"Successfully inserted {detailed_sales_df.count()} records into detailed_sales_summary")
        
        # Step 9: Cleanup - unpersist cached DataFrame
        # This replaces the DROP TABLE temp_sales_summary
        sales_summary_df.unpersist()
        
        logger.info("Sales data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing sales data: {str(e)}")
        raise e

# Example usage and main execution
if __name__ == "__main__":
    # Example parameters - replace with actual values or command line arguments
    start_date_param = "2023-01-01"
    end_date_param = "2023-12-31"
    
    # Execute the sales data processing
    process_sales_data(start_date_param, end_date_param)
    
    # Stop Spark session
    spark.stop()

# Additional utility functions for enhanced functionality

def validate_date_format(date_string: str) -> bool:
    """
    Validate date string format.
    
    Args:
        date_string (str): Date string to validate
    
    Returns:
        bool: True if valid format, False otherwise
    """
    try:
        from datetime import datetime
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def get_sales_summary_stats(start_date: str, end_date: str) -> dict:
    """
    Get summary statistics for the processed sales data.
    
    Args:
        start_date (str): Start date for analysis
        end_date (str): End date for analysis
    
    Returns:
        dict: Summary statistics
    """
    sales_df = spark.table("sales_table")
    
    filtered_df = sales_df.filter(
        (col("sale_date") >= lit(start_date)) & 
        (col("sale_date") <= lit(end_date))
    )
    
    stats = filtered_df.agg(
        spark_sum("sales").alias("total_sales"),
        col("product_id").count().alias("total_records"),
        col("product_id").countDistinct().alias("unique_products")
    ).collect()[0]
    
    return {
        "total_sales": stats["total_sales"],
        "total_records": stats["total_records"],
        "unique_products": stats["unique_products"]
    }

# Performance optimization configurations
def configure_spark_for_sales_processing():
    """
    Configure Spark settings optimized for sales data processing.
    """
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")