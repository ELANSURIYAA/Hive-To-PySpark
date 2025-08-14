from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col
from pyspark.sql.types import *
import logging

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ProcessSalesData") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_sales_data(start_date, end_date):
    """
    Convert Hive stored procedure to PySpark DataFrame operations.
    
    This function processes sales data between specified date ranges,
    creates summary tables, and performs detailed sales analysis.
    
    Args:
        start_date (str): Start date in string format (YYYY-MM-DD)
        end_date (str): End date in string format (YYYY-MM-DD)
    
    Returns:
        None: Results are written to target tables
    """
    
    try:
        logger.info(f"Starting sales data processing for period: {start_date} to {end_date}")
        
        # Read source sales table
        # Assuming sales_table exists as a Hive table or parquet files
        sales_df = spark.table("sales_table")
        
        # Validate input data
        if sales_df.count() == 0:
            logger.warning("No data found in sales_table")
            return
        
        # Filter sales data by date range (equivalent to WHERE clause in original query)
        filtered_sales_df = sales_df.filter(
            (col("sale_date") >= start_date) & 
            (col("sale_date") <= end_date)
        )
        
        logger.info(f"Filtered sales data count: {filtered_sales_df.count()}")
        
        # Create aggregated summary (equivalent to the dynamic query in original procedure)
        # GROUP BY product_id and SUM(sales) AS total_sales
        summary_df = filtered_sales_df.groupBy("product_id") \
            .agg(spark_sum("sales").alias("total_sales")) \
            .orderBy("product_id")
        
        # Cache the summary for multiple operations (optimization)
        summary_df.cache()
        
        logger.info(f"Summary data count: {summary_df.count()}")
        
        # Insert into summary_table (equivalent to INSERT INTO summary_table)
        # Mode 'overwrite' or 'append' based on business requirements
        summary_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("summary_table")
        
        logger.info("Data successfully inserted into summary_table")
        
        # Create temporary view for further processing
        # (equivalent to CREATE TEMPORARY TABLE temp_sales_summary)
        summary_df.createOrReplaceTempView("temp_sales_summary")
        
        # Process each row for detailed analysis
        # (equivalent to CURSOR operations in original procedure)
        # Convert cursor-based row processing to DataFrame operations
        
        # Collect summary data for row-by-row processing if needed
        # Note: collect() should be used carefully with large datasets
        summary_rows = summary_df.collect()
        
        # Prepare data for detailed_sales_summary table
        detailed_records = []
        
        for row in summary_rows:
            product_id = row['product_id']
            total_sales = row['total_sales']
            
            # Process each record (equivalent to cursor fetch and processing)
            if total_sales is not None:
                detailed_records.append((product_id, total_sales))
                logger.debug(f"Processed product_id: {product_id}, total_sales: {total_sales}")
        
        # Create DataFrame from processed records
        if detailed_records:
            detailed_schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("total_sales", FloatType(), True)
            ])
            
            detailed_df = spark.createDataFrame(detailed_records, detailed_schema)
            
            # Insert into detailed_sales_summary table
            detailed_df.write \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable("detailed_sales_summary")
            
            logger.info(f"Inserted {len(detailed_records)} records into detailed_sales_summary")
        
        # Alternative approach using DataFrame operations instead of collect()
        # This is more efficient for large datasets
        # summary_df.write \
        #     .mode("append") \
        #     .option("mergeSchema", "true") \
        #     .saveAsTable("detailed_sales_summary")
        
        # Clean up cached data
        summary_df.unpersist()
        
        # Drop temporary view (equivalent to DROP TABLE temp_sales_summary)
        spark.catalog.dropTempView("temp_sales_summary")
        
        logger.info("Sales data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in process_sales_data: {str(e)}")
        raise e

def main():
    """
    Main execution function with example usage
    """
    # Example usage
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
    Validate date format
    
    Args:
        date_string (str): Date string to validate
    
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        from datetime import datetime
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def get_sales_statistics(start_date, end_date):
    """
    Get additional sales statistics for the given period
    
    Args:
        start_date (str): Start date
        end_date (str): End date
    
    Returns:
        dict: Statistics dictionary
    """
    sales_df = spark.table("sales_table")
    
    filtered_df = sales_df.filter(
        (col("sale_date") >= start_date) & 
        (col("sale_date") <= end_date)
    )
    
    stats = {
        'total_records': filtered_df.count(),
        'unique_products': filtered_df.select("product_id").distinct().count(),
        'total_sales_amount': filtered_df.agg(spark_sum("sales")).collect()[0][0]
    }
    
    return stats

# Performance optimization notes:
# 1. Use broadcast joins for small lookup tables
# 2. Partition large tables by date for better performance
# 3. Use appropriate file formats (Parquet, Delta) for better I/O
# 4. Consider using Spark SQL for complex queries when DataFrame API becomes verbose
# 5. Monitor and tune Spark configurations based on cluster resources