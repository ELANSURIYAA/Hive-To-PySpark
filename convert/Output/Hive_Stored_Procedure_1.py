from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col, lit
from pyspark.sql.types import *
import logging

# Initialize Spark Session
def get_spark_session(app_name="HiveStoredProcedureConversion"):
    """
    Initialize and return Spark Session
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_sales_data(spark, start_date, end_date, 
                      sales_table_path, summary_table_path, 
                      detailed_sales_summary_path):
    """
    Convert Hive stored procedure logic to PySpark DataFrame operations
    
    Parameters:
    - spark: SparkSession object
    - start_date: Start date for filtering (STRING format)
    - end_date: End date for filtering (STRING format)
    - sales_table_path: Path to sales table
    - summary_table_path: Path to summary table for output
    - detailed_sales_summary_path: Path to detailed sales summary table
    """
    
    try:
        # Step 1: Read sales table
        print(f"Reading sales data from {sales_table_path}")
        sales_df = spark.read.table(sales_table_path) if sales_table_path.count('/') == 0 else spark.read.parquet(sales_table_path)
        
        # Step 2: Filter data based on date range (equivalent to WHERE clause)
        print(f"Filtering data between {start_date} and {end_date}")
        filtered_sales_df = sales_df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        # Step 3: Create aggregated summary (equivalent to temp_sales_summary table)
        print("Creating sales summary aggregation")
        temp_sales_summary_df = filtered_sales_df \
            .groupBy("product_id") \
            .agg(spark_sum("sales").alias("total_sales")) \
            .select("product_id", "total_sales")
        
        # Cache the temporary result for reuse (equivalent to temporary table)
        temp_sales_summary_df.cache()
        
        # Step 4: Write to summary_table (equivalent to INSERT INTO summary_table)
        print(f"Writing summary data to {summary_table_path}")
        temp_sales_summary_df.write \
            .mode("append") \
            .option("path", summary_table_path) \
            .saveAsTable("summary_table") if summary_table_path.count('/') == 0 else \
            temp_sales_summary_df.write.mode("append").parquet(summary_table_path)
        
        # Step 5: Process each record (equivalent to cursor logic)
        print("Processing individual records for detailed summary")
        
        # Collect data for processing (equivalent to cursor fetch)
        # Note: Use collect() carefully - only for small datasets
        # For large datasets, consider using foreachPartition() or other distributed operations
        summary_records = temp_sales_summary_df.collect()
        
        # Step 6: Create detailed records list (equivalent to cursor loop)
        detailed_records = []
        for row in summary_records:
            if row['total_sales'] is not None:
                detailed_records.append({
                    'product_id': row['product_id'],
                    'total_sales': row['total_sales']
                })
        
        # Step 7: Convert to DataFrame and write to detailed_sales_summary
        if detailed_records:
            print(f"Writing detailed summary to {detailed_sales_summary_path}")
            
            # Define schema for detailed sales summary
            detailed_schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("total_sales", FloatType(), True)
            ])
            
            detailed_df = spark.createDataFrame(detailed_records, detailed_schema)
            
            # Write detailed summary (equivalent to INSERT INTO detailed_sales_summary)
            detailed_df.write \
                .mode("append") \
                .option("path", detailed_sales_summary_path) \
                .saveAsTable("detailed_sales_summary") if detailed_sales_summary_path.count('/') == 0 else \
                detailed_df.write.mode("append").parquet(detailed_sales_summary_path)
        
        # Step 8: Unpersist cached DataFrame (equivalent to DROP TABLE temp_sales_summary)
        temp_sales_summary_df.unpersist()
        
        print("Sales data processing completed successfully")
        
        # Return summary statistics
        total_products = len(detailed_records)
        total_sales_amount = sum([record['total_sales'] for record in detailed_records])
        
        return {
            'status': 'success',
            'total_products_processed': total_products,
            'total_sales_amount': total_sales_amount,
            'date_range': f"{start_date} to {end_date}"
        }
        
    except Exception as e:
        print(f"Error processing sales data: {str(e)}")
        logging.error(f"Error in process_sales_data: {str(e)}")
        raise e

def main():
    """
    Main execution function - equivalent to stored procedure call
    """
    # Initialize Spark Session
    spark = get_spark_session()
    
    try:
        # Example usage - replace with actual parameters
        start_date = "2023-01-01"
        end_date = "2023-12-31"
        
        # Define table paths (adjust based on your environment)
        sales_table_path = "sales_table"  # or "/path/to/sales_table.parquet"
        summary_table_path = "summary_table"  # or "/path/to/summary_table.parquet"
        detailed_sales_summary_path = "detailed_sales_summary"  # or "/path/to/detailed_sales_summary.parquet"
        
        # Execute the conversion logic
        result = process_sales_data(
            spark=spark,
            start_date=start_date,
            end_date=end_date,
            sales_table_path=sales_table_path,
            summary_table_path=summary_table_path,
            detailed_sales_summary_path=detailed_sales_summary_path
        )
        
        print(f"Processing Result: {result}")
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        raise e
    finally:
        # Stop Spark Session
        spark.stop()

if __name__ == "__main__":
    main()

# Alternative approach for large datasets (avoiding collect())
def process_sales_data_distributed(spark, start_date, end_date, 
                                 sales_table_path, summary_table_path, 
                                 detailed_sales_summary_path):
    """
    Distributed version that avoids collect() for large datasets
    """
    try:
        # Steps 1-3: Same as above
        sales_df = spark.read.table(sales_table_path) if sales_table_path.count('/') == 0 else spark.read.parquet(sales_table_path)
        
        filtered_sales_df = sales_df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        temp_sales_summary_df = filtered_sales_df \
            .groupBy("product_id") \
            .agg(spark_sum("sales").alias("total_sales")) \
            .select("product_id", "total_sales")
        
        # Step 4: Write to summary table
        temp_sales_summary_df.write \
            .mode("append") \
            .option("path", summary_table_path) \
            .saveAsTable("summary_table") if summary_table_path.count('/') == 0 else \
            temp_sales_summary_df.write.mode("append").parquet(summary_table_path)
        
        # Step 5: Filter out null values and write directly (more efficient for large datasets)
        detailed_summary_df = temp_sales_summary_df.filter(col("total_sales").isNotNull())
        
        # Step 6: Write to detailed sales summary
        detailed_summary_df.write \
            .mode("append") \
            .option("path", detailed_sales_summary_path) \
            .saveAsTable("detailed_sales_summary") if detailed_sales_summary_path.count('/') == 0 else \
            detailed_summary_df.write.mode("append").parquet(detailed_sales_summary_path)
        
        print("Distributed sales data processing completed successfully")
        
        return {'status': 'success', 'processing_mode': 'distributed'}
        
    except Exception as e:
        print(f"Error in distributed processing: {str(e)}")
        raise e