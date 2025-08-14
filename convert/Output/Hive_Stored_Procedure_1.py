"""
PySpark DataFrame Implementation of Hive Stored Procedure: process_sales_data

This script converts the Hive stored procedure 'process_sales_data' to PySpark DataFrame operations.
The original procedure filters sales data by date range, aggregates by product_id, and inserts
results into summary and detailed tables.

Author: Data Engineering Team
Date: 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
import logging
from datetime import datetime
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "HiveToSparkMigration") -> SparkSession:
    """
    Create and configure Spark session with optimized settings.
    
    Args:
        app_name (str): Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def validate_date_format(date_string: str) -> bool:
    """
    Validate date string format (YYYY-MM-DD).
    
    Args:
        date_string (str): Date string to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def process_sales_data(spark: SparkSession, 
                      start_date: str, 
                      end_date: str,
                      sales_table_path: Optional[str] = None,
                      summary_table_path: Optional[str] = None,
                      detailed_summary_table_path: Optional[str] = None) -> None:
    """
    PySpark implementation of the Hive stored procedure 'process_sales_data'.
    
    This function performs the following operations:
    1. Filters sales data by date range
    2. Groups by product_id and aggregates sales
    3. Writes results to summary_table and detailed_sales_summary
    
    Args:
        spark (SparkSession): Active Spark session
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        sales_table_path (str, optional): Path to sales table data
        summary_table_path (str, optional): Path to write summary table
        detailed_summary_table_path (str, optional): Path to write detailed summary
    
    Raises:
        ValueError: If date format is invalid
        Exception: If data processing fails
    """
    
    # Validate input parameters
    if not validate_date_format(start_date):
        raise ValueError(f"Invalid start_date format: {start_date}. Expected YYYY-MM-DD")
    
    if not validate_date_format(end_date):
        raise ValueError(f"Invalid end_date format: {end_date}. Expected YYYY-MM-DD")
    
    logger.info(f"Processing sales data from {start_date} to {end_date}")
    
    try:
        # Read sales table data
        # In production, this would typically read from a data lake, database, or Hive table
        if sales_table_path:
            sales_df = spark.read.parquet(sales_table_path)
        else:
            # Assume sales_table is already registered as a temporary view or table
            sales_df = spark.table("sales_table")
        
        # Cache the source DataFrame for better performance since it's used multiple times
        sales_df.cache()
        
        logger.info(f"Loaded sales data with {sales_df.count()} records")
        
        # Filter sales data by date range
        # This replaces the WHERE clause in the original Hive query
        filtered_sales_df = sales_df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        logger.info(f"Filtered sales data contains {filtered_sales_df.count()} records")
        
        # Group by product_id and sum sales
        # This replaces the GROUP BY and SUM operations from the original query
        aggregated_sales_df = filtered_sales_df.groupBy("product_id") \
            .agg(spark_sum("sales").alias("total_sales")) \
            .orderBy("product_id")
        
        # Cache the aggregated DataFrame since it's used for both outputs
        aggregated_sales_df.cache()
        
        logger.info(f"Aggregated data contains {aggregated_sales_df.count()} product records")
        
        # Write to summary_table
        # This replaces the dynamic SQL INSERT statement from the original procedure
        logger.info("Writing data to summary_table")
        if summary_table_path:
            aggregated_sales_df.write \
                .mode("append") \
                .option("mergeSchema", "true") \
                .parquet(summary_table_path)
        else:
            # Write to Hive table or create temporary view
            aggregated_sales_df.write \
                .mode("append") \
                .saveAsTable("summary_table")
        
        # Write to detailed_sales_summary
        # This replaces the cursor logic and row-by-row INSERT operations
        # In Spark, we can perform bulk operations instead of iterating row by row
        logger.info("Writing data to detailed_sales_summary")
        if detailed_summary_table_path:
            aggregated_sales_df.write \
                .mode("append") \
                .option("mergeSchema", "true") \
                .parquet(detailed_summary_table_path)
        else:
            # Write to Hive table or create temporary view
            aggregated_sales_df.write \
                .mode("append") \
                .saveAsTable("detailed_sales_summary")
        
        # Show sample results for verification
        logger.info("Sample of processed data:")
        aggregated_sales_df.show(10, truncate=False)
        
        # Print summary statistics
        total_products = aggregated_sales_df.count()
        total_sales_amount = aggregated_sales_df.agg(spark_sum("total_sales")).collect()[0][0]
        
        logger.info(f"Processing completed successfully:")
        logger.info(f"- Total products processed: {total_products}")
        logger.info(f"- Total sales amount: {total_sales_amount}")
        
        # Unpersist cached DataFrames to free memory
        sales_df.unpersist()
        aggregated_sales_df.unpersist()
        
    except Exception as e:
        logger.error(f"Error processing sales data: {str(e)}")
        raise


def create_sample_data(spark: SparkSession) -> None:
    """
    Create sample sales data for testing purposes.
    
    Args:
        spark (SparkSession): Active Spark session
    """
    from pyspark.sql.functions import to_date
    
    # Sample data schema
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("sales", FloatType(), True),
        StructField("sale_date", StringType(), True)
    ])
    
    # Sample data
    sample_data = [
        ("PROD001", 1500.50, "2024-01-15"),
        ("PROD002", 2300.75, "2024-01-16"),
        ("PROD001", 1200.25, "2024-01-17"),
        ("PROD003", 3400.00, "2024-01-18"),
        ("PROD002", 1800.30, "2024-01-19"),
        ("PROD001", 2100.80, "2024-01-20"),
        ("PROD004", 950.40, "2024-01-21"),
        ("PROD003", 2750.60, "2024-01-22"),
        ("PROD002", 1650.90, "2024-01-23"),
        ("PROD004", 1100.20, "2024-01-24")
    ]
    
    # Create DataFrame
    sample_df = spark.createDataFrame(sample_data, schema)
    
    # Convert string dates to date type
    sample_df = sample_df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
    
    # Register as temporary view
    sample_df.createOrReplaceTempView("sales_table")
    
    logger.info("Sample sales data created and registered as 'sales_table'")


def main():
    """
    Main function to demonstrate the PySpark implementation.
    """
    # Create Spark session
    spark = create_spark_session("ProcessSalesDataMigration")
    
    try:
        # Create sample data for demonstration
        create_sample_data(spark)
        
        # Process sales data for a specific date range
        process_sales_data(
            spark=spark,
            start_date="2024-01-15",
            end_date="2024-01-22"
        )
        
        # Verify results by reading from the output tables
        logger.info("Verifying results from summary_table:")
        spark.table("summary_table").show()
        
        logger.info("Verifying results from detailed_sales_summary:")
        spark.table("detailed_sales_summary").show()
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise
    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    main()