#!/usr/bin/env python3
"""
Comprehensive Unit Tests for Hive_Stored_Procedure PySpark Code
Generated test suite covering happy paths, edge cases, and error handling

Test Categories:
- Happy Path Scenarios
- Edge Case Handling
- Error Handling
- Data Integrity
- Integration Testing
- Performance Comparison
- Spark Session Management
"""

import pytest
import logging
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql.utils import AnalysisException
import sys
import os
from datetime import datetime, date
import time

# Add the source code directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../convert/Output'))

# Import the functions to test
try:
    from Hive_Stored_Procedure_1 import process_sales_data, process_sales_data_optimized
except ImportError:
    # Mock the functions if import fails
    def process_sales_data(start_date, end_date):
        pass
    
    def process_sales_data_optimized(start_date, end_date):
        pass

class TestSalesDataProcessing:
    """
    Test suite for sales data processing functions
    """
    
    @classmethod
    def setup_class(cls):
        """
        Set up Spark session for all tests
        """
        cls.spark = SparkSession.builder \n            .appName("SalesDataProcessingTests") \n            .master("local[2]") \n            .config("spark.sql.adaptive.enabled", "true") \n            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \n            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \n            .getOrCreate()
        
        # Set log level to reduce noise during testing
        cls.spark.sparkContext.setLogLevel("WARN")
        
        # Create test schema
        cls.sales_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
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
        # Clean up any existing tables
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
                self.spark.catalog.dropTempView(table)
            except:
                pass
    
    def create_test_sales_data(self, data):
        """
        Helper method to create test sales data
        
        Args:
            data: List of tuples (product_id, sales, sale_date)
        """
        df = self.spark.createDataFrame(data, self.sales_schema)
        df.createOrReplaceTempView("sales_table")
        return df
    
    def create_target_tables(self):
        """
        Helper method to create target tables for testing
        """
        # Create empty summary_table
        summary_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("total_sales", FloatType(), True)
        ])
        empty_summary_df = self.spark.createDataFrame([], summary_schema)
        empty_summary_df.write.mode("overwrite").saveAsTable("summary_table")
        
        # Create empty detailed_sales_summary table
        empty_detailed_df = self.spark.createDataFrame([], summary_schema)
        empty_detailed_df.write.mode("overwrite").saveAsTable("detailed_sales_summary")

    # ========================================
    # HAPPY PATH TEST CASES
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
        with patch('Hive_Stored_Procedure_1.spark', self.spark):
            process_sales_data("2023-06-15", "2023-06-17")
        
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
        with patch('Hive_Stored_Procedure_1.spark', self.spark):
            process_sales_data("2023-06-15", "2023-06-15")
        
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
        with patch('Hive_Stored_Procedure_1.spark', self.spark):
            process_sales_data_optimized("2023-03-01", "2023-03-31")
        
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
        with patch('Hive_Stored_Procedure_1.spark', self.spark):
            process_sales_data("2023-01-01", "2023-01-31")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        
        prod001_total = next(row.total_sales for row in summary_result if row.product_id == "PROD001")
        prod002_total = next(row.total_sales for row in summary_result if row.product_id == "PROD002")
        
        assert prod001_total == 600.0  # 100 + 200 + 300
        assert prod002_total == 400.0  # 150 + 250

    # ========================================
    # EDGE CASE TEST CASES
    # ========================================
    
    def test_tc_005_empty_sales_table(self):
        """
        TC_005: Test behavior when sales_table is empty
        """
        # Arrange
        self.create_test_sales_data([])  # Empty data
        self.create_target_tables()
        
        # Act
        with patch('Hive_Stored_Procedure_1.spark', self.spark):
            process_sales_data("2023-01-01", "2023-12-31")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        detailed_result = self.spark.table("detailed_sales_summary").collect()
        
        assert len(summary_result) == 0
        assert len(detailed_result) == 0
    
    def test_tc_006_no_data_in_date_range(self):
        """
        TC_006: Test when no sales data exists within specified date range
        """
        # Arrange
        test_data = [
            ("PROD001", 100.0, date(2022, 6, 15)),
            ("PROD002", 200.0, date(2022, 6, 16))
        ]
        self.create_test_sales_data(test_data)
        self.create_target_tables()
        
        # Act
        with patch('Hive_Stored_Procedure_1.spark', self.spark):
            process_sales_data("2025-01-01", "2025-12-31")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        detailed_result = self.spark.table("detailed_sales_summary").collect()
        
        assert len(summary_result) == 0
        assert len(detailed_result) == 0
    
    def test_tc_007_null_sales_values(self):
        """
        TC_007: Test handling of null values in sales column
        """
        # Arrange
        test_data = [
            ("PROD001", 100.0, date(2023, 6, 15)),
            ("PROD002", None, date(2023, 6, 16)),
            ("PROD001", 150.0, date(2023, 6, 17))
        ]
        self.create_test_sales_data(test_data)
        self.create_target_tables()
        
        # Act
        with patch('Hive_Stored_Procedure_1.spark', self.spark):
            process_sales_data("2023-06-15", "2023-06-17")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        
        # PROD001 should have total of 250.0, PROD002 should have null (handled by Spark aggregation)
        prod001_total = next((row.total_sales for row in summary_result if row.product_id == "PROD001"), None)
        assert prod001_total == 250.0
    
    def test_tc_008_null_product_ids(self):
        """
        TC_008: Test behavior with null product_id values
        """
        # Arrange
        test_data = [
            ("PROD001", 100.0, date(2023, 6, 15)),
            (None, 200.0, date(2023, 6, 16