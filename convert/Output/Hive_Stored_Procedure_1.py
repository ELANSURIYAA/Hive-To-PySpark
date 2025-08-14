from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col, lit
from pyspark.sql.types import *
import logging

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SalesDataProcessing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_sales_data(start_date, end_date):
    """
    Process sales data for a given date range.
    
    This function replicates the Hive stored procedure logic:
    1. Aggregates sales data by product_id within the specified date range
    2. Inserts aggregated results into summary_table
    3. Creates detailed sales summary with row-by-row processing
    
    Args:
        start_date (str): Start date in string format (e.g., '2023-01-01')
        end_date (str): End date in string format (e.g., '2023-12-31')
    
    Returns:
        None: Results are written to target tables
    """
    
    try:
        logger.info(f"Starting sales data processing for date range: {start_date} to {end_date}")
        
        # Read source sales table
        # Note: Replace 'sales_table' with actual table path or data source
        sales_df = spark.table("sales_table")
        
        # Filter sales data by date range (equivalent to WHERE clause in Hive)
        filtered_sales_df = sales_df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        # Aggregate sales by product_id (equivalent to GROUP BY in Hive)
        aggregated_sales_df = filtered_sales_df.groupBy("product_id") \
            .agg(spark_sum("sales").alias("total_sales"))
        
        # Cache the aggregated result for reuse
        aggregated_sales_df.cache()
        
        logger.info(f"Aggregated {aggregated_sales_df.count()} product records")
        
        # Insert into summary_table (equivalent to dynamic INSERT in Hive)
        # Mode 'append' to add new records, use 'overwrite' if needed
        aggregated_sales_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("summary_table")
        
        logger.info("Successfully inserted aggregated data into summary_table")
        
        # Create temporary sales summary (equivalent to CREATE TEMPORARY TABLE)
        # Register as temporary view for further processing
        temp_sales_summary_df = aggregated_sales_df.select(
            col("product_id"),
            col("total_sales")
        )
        
        temp_sales_summary_df.createOrReplaceTempView("temp_sales_summary")
        
        # Process each row for detailed sales summary
        # (equivalent to cursor operations in Hive)
        # Collect data for row-by-row processing (use with caution for large datasets)
        sales_summary_rows = temp_sales_summary_df.collect()
        
        # Prepare data for batch insert into detailed_sales_summary
        detailed_records = []
        
        for row in sales_summary_rows:
            product_id = row['product_id']
            total_sales = row['total_sales']
            
            # Only process non-null total_sales (equivalent to WHILE condition)
            if total_sales is not None:
                detailed_records.append((product_id, total_sales))
                logger.debug(f"Processed product_id: {product_id}, total_sales: {total_sales}")
        
        # Create DataFrame from collected records
        if detailed_records:
            detailed_schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("total_sales", FloatType(), True)
            ])
            
            detailed_sales_df = spark.createDataFrame(detailed_records, detailed_schema)
            
            # Insert into detailed_sales_summary table
            detailed_sales_df.write \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable("detailed_sales_summary")
            
            logger.info(f"Successfully inserted {len(detailed_records)} records into detailed_sales_summary")
        
        # Clean up cached data (equivalent to DROP TABLE temp_sales_summary)
        aggregated_sales_df.unpersist()
        spark.catalog.dropTempView("temp_sales_summary")
        
        logger.info("Sales data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing sales data: {str(e)}")
        raise e

# Alternative implementation using DataFrame operations for better performance
# This approach avoids collect() operation for large datasets
def process_sales_data_optimized(start_date, end_date):
    """
    Optimized version of sales data processing using pure DataFrame operations.
    Recommended for large datasets to avoid driver memory issues.
    
    Args:
        start_date (str): Start date in string format
        end_date (str): End date in string format
    """
    
    try:
        logger.info(f"Starting optimized sales data processing for date range: {start_date} to {end_date}")
        
        # Read and filter sales data
        sales_df = spark.table("sales_table")
        
        filtered_sales_df = sales_df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        # Aggregate sales by product_id
        aggregated_sales_df = filtered_sales_df.groupBy("product_id") \
            .agg(spark_sum("sales").alias("total_sales"))
        
        # Insert into summary_table
        aggregated_sales_df.write \
            .mode("append") \
            .saveAsTable("summary_table")
        
        # Insert into detailed_sales_summary (same data, different table)
        # Filter out null total_sales values
        detailed_sales_df = aggregated_sales_df.filter(col("total_sales").isNotNull())
        
        detailed_sales_df.write \
            .mode("append") \
            .saveAsTable("detailed_sales_summary")
        
        logger.info("Optimized sales data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in optimized processing: {str(e)}")
        raise e

# Main execution
if __name__ == "__main__":
    # Example usage
    start_date = "2023-01-01"
    end_date = "2023-12-31"
    
    # Use the standard implementation (replicates exact Hive logic)
    process_sales_data(start_date, end_date)
    
    # Alternative: Use optimized implementation for better performance
    # process_sales_data_optimized(start_date, end_date)
    
    # Stop Spark session
    spark.stop()

# Configuration Notes:
# 1. Ensure 'sales_table', 'summary_table', and 'detailed_sales_summary' tables exist
# 2. Adjust data types based on actual schema requirements
# 3. Consider partitioning strategies for large datasets
# 4. Monitor memory usage when using collect() operations
# 5. Use broadcast joins if dimension tables are involved