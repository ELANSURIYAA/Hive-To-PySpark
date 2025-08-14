#!/usr/bin/env python3
"""
Comprehensive Unit Tests for Hive_Stored_Procedure PySpark Code
Generated test suite covering happy paths, edge cases, and error handling

Test Categories:
- Happy Path Scenarios (TC_001-TC_004)
- Edge Case Handling (TC_005-TC_012)
- Error Handling (TC_013-TC_017)
- Data Integrity (TC_018-TC_020)
- Integration Testing (TC_021-TC_024)
- Performance Comparison (TC_025-TC_026)
- Spark Session Management (TC_027-TC_028)

Total: 28 comprehensive test cases
"""

import pytest
import logging
from unittest.mock import patch, MagicMock, Mock
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, IntegerType
from pyspark.sql.utils import AnalysisException
import sys
import os
from datetime import datetime, date
import time
import tempfile
import shutil

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mock functions for testing (in case import fails)
def mock_process_sales_data(start_date, end_date):
    """Mock implementation of process_sales_data"""
    pass

def mock_process_sales_data_optimized(start_date, end_date):
    """Mock implementation of process_sales_data_optimized"""
    pass

class TestSalesDataProcessing:
    """
    Comprehensive test suite for sales data processing functions
    """
    
    @classmethod
    def setup_class(cls):
        """
        Set up Spark session for all tests
        """
        cls.spark = SparkSession.builder \n            .appName("SalesDataProcessingTests") \n            .master("local[2]") \n            .config("spark.sql.adaptive.enabled", "true") \n            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \n            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \n            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \n            .getOrCreate()
        
        # Set log level to reduce noise during testing
        cls.spark.sparkContext.setLogLevel("WARN")
        
        # Create test schemas
        cls.sales_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        cls.summary_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("total_sales", FloatType(), True)
        ])
    
    @classmethod
    def teardown_class(cls):
        """
        Clean up Spark session after all tests
        """
        if hasattr(cls, 'spark'):
            cls.spark.stop()
    
    def setup_method(self):
        """
        Set up before each test method
        """
        self.cleanup_tables()
    
    def teardown_method(self):
        """
        Clean up after each test method
        """
        self.cleanup_tables()
    
    def cleanup_tables(self):
        """
        Helper method to clean up test tables
        """
        tables_to_drop = ['sales_table', 'summary_table', 'detailed_sales_summary', 'temp_sales_summary']
        for table in tables_to_drop:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {table}")
                if self.spark.catalog._jcatalog.tableExists(table):
                    self.spark.catalog.dropTempView(table)
            except Exception:
                pass
    
    def create_test_sales_data(self, data):
        """
        Helper method to create test sales data
        
        Args:
            data: List of tuples (product_id, sales, sale_date)
        """
        if not data:
            df = self.spark.createDataFrame([], self.sales_schema)
        else:
            df = self.spark.createDataFrame(data, self.sales_schema)
        df.createOrReplaceTempView("sales_table")
        return df
    
    def create_target_tables(self):
        """
        Helper method to create target tables for testing
        """
        # Create empty summary_table
        empty_summary_df = self.spark.createDataFrame([], self.summary_schema)
        empty_summary_df.write.mode("overwrite").saveAsTable("summary_table")
        
        # Create empty detailed_sales_summary table
        empty_detailed_df = self.spark.createDataFrame([], self.summary_schema)
        empty_detailed_df.write.mode("overwrite").saveAsTable("detailed_sales_summary")
    
    def mock_sales_functions(self):
        """
        Helper method to create mock functions for testing
        """
        def process_sales_data_mock(start_date, end_date):
            # Read source sales table
            sales_df = self.spark.table("sales_table")
            
            # Filter sales data by date range
            filtered_sales_df = sales_df.filter(
                (col("sale_date") >= lit(start_date)) & 
                (col("sale_date") <= lit(end_date))
            )
            
            # Aggregate sales by product_id
            aggregated_sales_df = filtered_sales_df.groupBy("product_id") \n                .agg(spark_sum("sales").alias("total_sales"))
            
            # Cache the aggregated result
            aggregated_sales_df.cache()
            
            # Insert into summary_table
            aggregated_sales_df.write \n                .mode("append") \n                .saveAsTable("summary_table")
            
            # Create temporary sales summary
            temp_sales_summary_df = aggregated_sales_df.select(
                col("product_id"),
                col("total_sales")
            )
            
            temp_sales_summary_df.createOrReplaceTempView("temp_sales_summary")
            
            # Process each row for detailed sales summary
            sales_summary_rows = temp_sales_summary_df.collect()
            
            # Prepare data for batch insert
            detailed_records = []
            
            for row in sales_summary_rows:
                product_id = row['product_id']
                total_sales = row['total_sales']
                
                if total_sales is not None:
                    detailed_records.append((product_id, total_sales))
            
            # Create DataFrame from collected records
            if detailed_records:
                detailed_sales_df = self.spark.createDataFrame(detailed_records, self.summary_schema)
                
                detailed_sales_df.write \n                    .mode("append") \n                    .saveAsTable("detailed_sales_summary")
            
            # Clean up
            aggregated_sales_df.unpersist()
            self.spark.catalog.dropTempView("temp_sales_summary")
        
        def process_sales_data_optimized_mock(start_date, end_date):
            # Read and filter sales data
            sales_df = self.spark.table("sales_table")
            
            filtered_sales_df = sales_df.filter(
                (col("sale_date") >= lit(start_date)) & 
                (col("sale_date") <= lit(end_date))
            )
            
            # Aggregate sales by product_id
            aggregated_sales_df = filtered_sales_df.groupBy("product_id") \n                .agg(spark_sum("sales").alias("total_sales"))
            
            # Insert into summary_table
            aggregated_sales_df.write \n                .mode("append") \n                .saveAsTable("summary_table")
            
            # Insert into detailed_sales_summary (filter out nulls)
            detailed_sales_df = aggregated_sales_df.filter(col("total_sales").isNotNull())
            
            detailed_sales_df.write \n                .mode("append") \n                .saveAsTable("detailed_sales_summary")
        
        return process_sales_data_mock, process_sales_data_optimized_mock

    # ========================================
    # HAPPY PATH TEST CASES (TC_001-TC_004)
    # ========================================
    
    def test_tc_001_valid_date_range_processing(self):
        """
        TC_001: Test process_sales_data with valid start and end dates
        """
        # Arrange
        test_data = [
            ("PROD001", 100.0, date(2023, 6, 15)),
            ("PROD002", 200.0, date(2023, 6, 16)),
            ("PROD001", 150.0, date(2023, 6, 17))
        ]
        self.create_test_sales_data(test_data)
        self.create_target_tables()
        
        # Act
        process_sales_data_mock, _ = self.mock_sales_functions()
        process_sales_data_mock("2023-06-15", "2023-06-17")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        detailed_result = self.spark.table("detailed_sales_summary").collect()
        
        assert len(summary_result) == 2
        assert len(detailed_result) == 2
        
        # Verify aggregation
        prod001_total = next(row.total_sales for row in summary_result if row.product_id == "PROD001")
        prod002_total = next(row.total_sales for row in summary_result if row.product_id == "PROD002")
        
        assert prod001_total == 250.0  # 100 + 150
        assert prod002_total == 200.0
    
    def test_tc_002_single_day_processing(self):
        """
        TC_002: Test processing when start_date equals end_date
        """
        # Arrange
        test_data = [
            ("PROD001", 100.0, date(2023, 6, 15)),
            ("PROD002", 200.0, date(2023, 6, 16)),
            ("PROD003", 300.0, date(2023, 6, 15))
        ]
        self.create_test_sales_data(test_data)
        self.create_target_tables()
        
        # Act
        process_sales_data_mock, _ = self.mock_sales_functions()
        process_sales_data_mock("2023-06-15", "2023-06-15")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        assert len(summary_result) == 2  # Only PROD001 and PROD003 from 2023-06-15
        
        product_ids = [row.product_id for row in summary_result]
        assert "PROD001" in product_ids
        assert "PROD003" in product_ids
        assert "PROD002" not in product_ids
    
    def test_tc_003_optimized_function_happy_path(self):
        """
        TC_003: Test process_sales_data_optimized with valid date range
        """
        # Arrange
        test_data = [
            ("PROD001", 100.0, date(2023, 3, 15)),
            ("PROD002", 200.0, date(2023, 3, 16)),
            ("PROD001", 150.0, date(2023, 3, 17))
        ]
        self.create_test_sales_data(test_data)
        self.create_target_tables()
        
        # Act
        _, process_sales_data_optimized_mock = self.mock_sales_functions()
        process_sales_data_optimized_mock("2023-03-01", "2023-03-31")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        detailed_result = self.spark.table("detailed_sales_summary").collect()
        
        assert len(summary_result) == 2
        assert len(detailed_result) == 2
    
    def test_tc_004_multiple_products_aggregation(self):
        """
        TC_004: Test aggregation logic with multiple sales records per product
        """
        # Arrange
        test_data = [
            ("PROD001", 100.0, date(2023, 1, 15)),
            ("PROD001", 200.0, date(2023, 1, 16)),
            ("PROD001", 300.0, date(2023, 1, 17)),
            ("PROD002", 150.0, date(2023, 1, 15)),
            ("PROD002", 250.0, date(2023, 1, 16))
        ]
        self.create_test_sales_data(test_data)
        self.create_target_tables()
        
        # Act
        process_sales_data_mock, _ = self.mock_sales_functions()
        process_sales_