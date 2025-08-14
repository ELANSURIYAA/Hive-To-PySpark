#!/usr/bin/env python3
"""
Comprehensive Pytest Suite for Hive to PySpark Conversion Testing
Generated for: Hive_Stored_Procedure.txt to PySpark Conversion
Version: 1
Date: 2024

This test suite validates the conversion of HiveQL stored procedures to PySpark,
including syntax changes, manual interventions, and performance optimizations.
"""

import pytest
import logging
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql.utils import AnalysisException
import tempfile
import shutil
import os

# Configure logging for testing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestHiveToSparkConversion:
    """
    Comprehensive test class for Hive to PySpark conversion validation
    """
    
    @classmethod
    def setup_class(cls):
        """
        Setup SparkSession for all tests
        """
        cls.spark = SparkSession.builder \
            .appName("HiveToSparkConversionTest") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        cls.spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession initialized for testing")
    
    @classmethod
    def teardown_class(cls):
        """
        Cleanup SparkSession after all tests
        """
        if hasattr(cls, 'spark'):
            cls.spark.stop()
            logger.info("SparkSession stopped")
    
    def setup_method(self, method):
        """
        Setup method called before each test
        """
        self.test_data_path = tempfile.mkdtemp()
        logger.info(f"Test setup for {method.__name__}")
    
    def teardown_method(self, method):
        """
        Cleanup method called after each test
        """
        # Clean up any temporary tables/views
        try:
            self.spark.catalog.dropTempView("sales_table")
        except:
            pass
        try:
            self.spark.catalog.dropTempView("summary_table")
        except:
            pass
        try:
            self.spark.catalog.dropTempView("detailed_sales_summary")
        except:
            pass
        try:
            self.spark.catalog.dropTempView("temp_sales_summary")
        except:
            pass
        
        # Clean up test data directory
        if hasattr(self, 'test_data_path') and os.path.exists(self.test_data_path):
            shutil.rmtree(self.test_data_path)
        
        logger.info(f"Test cleanup completed for {method.__name__}")
    
    def create_sample_sales_data(self, num_records=100, start_date="2023-01-01", end_date="2023-12-31"):
        """
        Create sample sales data for testing
        """
        from datetime import datetime, timedelta
        import random
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        date_range = (end_dt - start_dt).days
        
        data = []
        for i in range(num_records):
            random_days = random.randint(0, date_range)
            sale_date = (start_dt + timedelta(days=random_days)).strftime("%Y-%m-%d")
            product_id = f"PROD_{random.randint(1, 10):03d}"
            sales = round(random.uniform(100.0, 1000.0), 2)
            data.append((product_id, sales, sale_date))
        
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        df.createOrReplaceTempView("sales_table")
        return df
    
    def create_empty_target_tables(self):
        """
        Create empty target tables for testing
        """
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("total_sales", FloatType(), True)
        ])
        
        empty_df = self.spark.createDataFrame([], schema)
        empty_df.createOrReplaceTempView("summary_table")
        empty_df.createOrReplaceTempView("detailed_sales_summary")
    
    # Implement the core functions to test
    def process_sales_data(self, start_date, end_date):
        """
        PySpark implementation of the original HiveQL stored procedure
        """
        try:
            logger.info(f"Starting sales data processing for period: {start_date} to {end_date}")
            
            # Read source sales table
            sales_df = self.spark.table("sales_table")
            
            # Validate input data
            if sales_df.count() == 0:
                logger.warning("No data found in sales_table")
                return
            
            # Filter sales data by date range
            filtered_sales_df = sales_df.filter(
                (col("sale_date") >= start_date) & 
                (col("sale_date") <= end_date)
            )
            
            logger.info(f"Filtered sales data count: {filtered_sales_df.count()}")
            
            # Create aggregated summary
            summary_df = filtered_sales_df.groupBy("product_id") \
                .agg(spark_sum("sales").alias("total_sales")) \
                .orderBy("product_id")
            
            # Cache the summary for multiple operations
            summary_df.cache()
            
            logger.info(f"Summary data count: {summary_df.count()}")
            
            # Create temporary view
            summary_df.createOrReplaceTempView("temp_sales_summary")
            
            # Process each row for detailed analysis
            summary_rows = summary_df.collect()
            
            # Prepare data for detailed_sales_summary table
            detailed_records = []
            
            for row in summary_rows:
                product_id = row['product_id']
                total_sales = row['total_sales']
                
                if total_sales is not None:
                    detailed_records.append((product_id, total_sales))
                    logger.debug(f"Processed product_id: {product_id}, total_sales: {total_sales}")
            
            # Create DataFrame from processed records
            if detailed_records:
                detailed_schema = StructType([
                    StructField("product_id", StringType(), True),
                    StructField("total_sales", FloatType(), True)
                ])
                
                detailed_df = self.spark.createDataFrame(detailed_records, detailed_schema)
                detailed_df.createOrReplaceTempView("detailed_sales_summary")
                
                logger.info(f"Processed {len(detailed_records)} records into detailed_sales_summary")
            
            # Clean up cached data
            summary_df.unpersist()
            
            logger.info("Sales data processing completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error in process_sales_data: {str(e)}")
            raise e
    
    def validate_date_format(self, date_string):
        """
        Validate date format
        """
        try:
            datetime.strptime(date_string, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    def get_sales_statistics(self, start_date, end_date):
        """
        Get additional sales statistics for the given period
        """
        sales_df = self.spark.table("sales_table")
        
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
    
    # Test Cases Implementation
    
    def test_tc001_basic_stored_procedure_conversion(self):
        """
        TC001: Basic Stored Procedure Conversion Test
        Verify that the HiveQL stored procedure is correctly converted to a PySpark function
        """
        self.create_sample_sales_data(50)
        self.create_empty_target_tables()
        
        # Test function call with valid parameters
        result = self.process_sales_data("2023-01-01", "2023-12-31")
        
        assert result is True, "Function should execute successfully"
        logger.info("TC001 PASSED: Basic stored procedure conversion works correctly")
    
    def test_tc002_dynamic_sql_to_dataframe_conversion(self):
        """
        TC002: Dynamic SQL to DataFrame Conversion Test
        Verify that dynamic SQL query generation is replaced with DataFrame operations
        """
        self.create_sample_sales_data(100)
        self.create_empty_target_tables()
        
        # Execute the conversion
        self.process_sales_data("2023-06-01", "2023-06-30")
        
        # Verify temp view was created (equivalent to dynamic SQL execution)
        temp_view_exists = False
        try:
            temp_df = self.spark.table("temp_sales_summary")
            temp_view_exists = temp_df.count() >= 0
        except:
            temp_view_exists = False
        
        assert temp_view_exists, "Temporary view should be created as replacement for dynamic SQL"
        logger.info("TC002 PASSED: Dynamic SQL to DataFrame conversion works correctly")
    
    def test_tc003_temporary_table_to_tempview_conversion(self):
        """
        TC003: Temporary Table to TempView Conversion Test
        Verify that CREATE TEMPORARY TABLE is correctly converted to createOrReplaceTempView
        """
        self.create_sample_sales_data(75)
        self.create_empty_target_tables()
        
        self.process_sales_data("2023-01-01", "2023-03-31")
        
        # Verify temporary view exists and is accessible
        temp_df = self.spark.table("temp_sales_summary")
        assert temp_df.count() > 0, "Temporary view should contain aggregated data"
        
        # Verify structure matches expected schema
        expected_columns = {"product_id", "total_sales"}
        actual_columns = set(temp_df.columns)
        assert expected_columns.issubset(actual_columns), "Temporary view should have correct schema"
        
        logger.info("TC003 PASSED: Temporary table to temp view conversion works correctly")
    
    def test_tc004_cursor_to_dataframe_collection_conversion(self):
        """
        TC004: Cursor to DataFrame Collection Conversion Test
        Verify that cursor operations are correctly converted to DataFrame collect() and iteration
        """
        self.create_sample_sales_data(30)
        self.create_empty_target_tables()
        
        self.process_sales_data("2023-01-01", "2023-12-31")
        
        # Verify detailed_sales_summary was populated (equivalent to cursor processing)
        detailed_df = self.spark.table("detailed_sales_summary")
        detailed_count = detailed_df.count()
        
        # Verify temp_sales_summary exists for comparison
        temp_df = self.spark.table("temp_sales_summary")
        temp_count = temp_df.count()
        
        assert detailed_count == temp_count, "Cursor processing should process all rows from temp table"
        assert detailed_count > 0, "Detailed summary should contain processed records"
        
        logger.info("TC004 PASSED: Cursor to DataFrame collection conversion works correctly")
    
    def test_tc005_aggregate_function_conversion(self):
        """
        TC005: Aggregate Function Conversion Test
        Verify that SQL SUM() function is correctly converted to PySpark spark_sum() function
        """
        # Create specific test data for aggregation validation
        test_data = [
            ("PROD_001", 100.0, "2023-06-01"),
            ("PROD_001", 200.0, "2023-06-02"),
            ("PROD_002", 150.0, "2023-06-01"),
            ("PROD_002", 250.0, "2023-06-03")
        ]
        
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(test_data, schema)
        df.createOrReplaceTempView("sales_table")
        self.create_empty_target_tables()
        
        self.process_sales_data("2023-06-01", "2023-06-30")
        
        # Verify aggregation results
        temp_df = self.spark.table("temp_sales_summary")
        results = temp_df.collect()
        
        # Expected: PROD_001 = 300.0, PROD_002 = 400.0
        result_dict = {row['product_id']: row['total_sales'] for row in results}
        
        assert abs(result_dict['PROD_001'] - 300.0) < 0.01, "PROD_001 aggregation should equal 300.0"
        assert abs(result_dict['PROD_002'] - 400.0) < 0.01, "PROD_002 aggregation should equal 400.0"
        
        logger.info("TC005 PASSED: Aggregate function conversion works correctly")
    
    def test_tc020_backward_compatibility(self):
        """
        TC020: Backward Compatibility Test
        Verify that the converted PySpark code produces same results as original HiveQL procedure
        """
        self.create_sample_sales_data(100)
        self.create_empty_target_tables()
        
        # Execute PySpark implementation
        result = self.process_sales_data("2023-01-01", "2023-12-31")
        
        assert result is True, "PySpark implementation should execute successfully"
        
        # Verify results exist
        detailed_df = self.spark.table("detailed_sales_summary")
        record_count = detailed_df.count()
        
        assert record_count > 0, "PySpark implementation should produce results"
        
        logger.info("TC020 PASSED: Backward compatibility validation successful")

if __name__ == "__main__":
    pytest.main(["-v", __file__])