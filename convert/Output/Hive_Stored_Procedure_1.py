from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

def process_sales_data(start_date, end_date):
    """
    PySpark equivalent of Hive stored procedure process_sales_data
    
    Parameters:
    start_date (str): Start date for sales data processing (format: 'YYYY-MM-DD')
    end_date (str): End date for sales data processing (format: 'YYYY-MM-DD')
    
    Returns:
    None: Function performs data processing and writes results to tables
    """
    
    try:
        logger.info(f"Starting sales data processing for period: {start_date} to {end_date}")
        
        # Read sales_table - assuming it exists as a Delta/Parquet table
        # Replace with actual table path/database reference
        sales_df = spark.table("sales_table")
        
        # Validate input parameters
        if not start_date or not end_date:
            raise ValueError("Start date and end date must be provided")
        
        # Filter sales data by date range
        # Equivalent to: WHERE sale_date BETWEEN start_date AND end_date
        filtered_sales_df = sales_df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        # Create temporary sales summary
        # Equivalent to: CREATE TEMPORARY TABLE temp_sales_summary AS
        # SELECT product_id, SUM(sales) AS total_sales FROM sales_table
        # WHERE sale_date BETWEEN start_date AND end_date GROUP BY product_id
        temp_sales_summary_df = filtered_sales_df \
            .groupBy("product_id") \
            .agg(sum("sales").alias("total_sales")) \
            .cache()  # Cache for reuse
        
        logger.info(f"Processed {temp_sales_summary_df.count()} product records")
        
        # Insert aggregated data into summary_table
        # Equivalent to: INSERT INTO summary_table SELECT product_id, SUM(sales) AS total_sales
        # FROM sales_table WHERE sale_date BETWEEN start_date AND end_date GROUP BY product_id
        temp_sales_summary_df \
            .write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("summary_table")
        
        logger.info("Successfully inserted data into summary_table")
        
        # Process each record for detailed_sales_summary
        # Equivalent to cursor processing in Hive stored procedure
        # DECLARE cur CURSOR FOR SELECT product_id, total_sales FROM temp_sales_summary
        
        # Collect data for row-by-row processing (equivalent to cursor)
        # Note: Use collect() carefully with large datasets
        sales_summary_records = temp_sales_summary_df.collect()
        
        # Prepare data for batch insert into detailed_sales_summary
        detailed_records = []
        
        # Process each record (equivalent to cursor FETCH and WHILE loop)
        for row in sales_summary_records:
            product_id = row['product_id']
            total_sales = row['total_sales']
            
            # Equivalent to: INSERT INTO detailed_sales_summary (product_id, total_sales)
            # VALUES (product_id, total_sales)
            if total_sales is not None:
                detailed_records.append((product_id, total_sales))
        
        # Create DataFrame for batch insert
        if detailed_records:
            detailed_schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("total_sales", FloatType(), True)
            ])
            
            detailed_df = spark.createDataFrame(detailed_records, detailed_schema)
            
            # Insert into detailed_sales_summary table
            detailed_df \
                .write \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable("detailed_sales_summary")
            
            logger.info(f"Successfully inserted {len(detailed_records)} records into detailed_sales_summary")
        
        # Cleanup: Unpersist cached DataFrame (equivalent to DROP TABLE temp_sales_summary)
        temp_sales_summary_df.unpersist()
        
        logger.info("Sales data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in process_sales_data: {str(e)}")
        raise e

# Alternative implementation using DataFrame operations for better performance
def process_sales_data_optimized(start_date, end_date):
    """
    Optimized PySpark version using pure DataFrame operations
    Avoids collect() for better performance with large datasets
    """
    
    try:
        logger.info(f"Starting optimized sales data processing for period: {start_date} to {end_date}")
        
        # Read sales_table
        sales_df = spark.table("sales_table")
        
        # Filter and aggregate in one operation
        sales_summary_df = sales_df \
            .filter(
                (col("sale_date") >= lit(start_date)) & 
                (col("sale_date") <= lit(end_date))
            ) \
            .groupBy("product_id") \
            .agg(sum("sales").alias("total_sales")) \
            .filter(col("total_sales").isNotNull())
        
        # Write to summary_table
        sales_summary_df \
            .write \
            .mode("append") \
            .saveAsTable("summary_table")
        
        # Write to detailed_sales_summary (same data, different table)
        sales_summary_df \
            .write \
            .mode("append") \
            .saveAsTable("detailed_sales_summary")
        
        logger.info("Optimized sales data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in optimized process_sales_data: {str(e)}")
        raise e

# Main execution
if __name__ == "__main__":
    # Example usage
    start_date = "2023-01-01"
    end_date = "2023-12-31"
    
    # Use the standard version (closer to original stored procedure logic)
    process_sales_data(start_date, end_date)
    
    # Or use the optimized version for better performance
    # process_sales_data_optimized(start_date, end_date)
    
    # Stop Spark session
    spark.stop()

# Configuration notes:
# 1. Ensure sales_table, summary_table, and detailed_sales_summary tables exist
# 2. Adjust table paths/database references as per your environment
# 3. Consider partitioning strategies for large datasets
# 4. Monitor performance and adjust Spark configurations as needed
# 5. Implement proper error handling and logging for production use