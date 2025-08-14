#!/usr/bin/env python3
"""
Comprehensive Unit Tests for Hive_Stored_Procedure PySpark Code
Generated for process_sales_data functions
Version: 1
Date: Auto-generated

This test suite provides comprehensive coverage for:
- process_sales_data()
- process_sales_data_with_validation()
- process_sales_data_sql_approach()

Test Categories:
- Happy path scenarios
- Edge cases
- Error handling and validation
- Data transformation validation
- Caching and performance
- SQL approach testing
- Integration and end-to-end
- Schema and data type validation
"""

import pytest
import logging
from datetime import datetime, date
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, DateType, IntegerType
)
from pyspark.sql.utils import AnalysisException

# Import the functions to test
# Note: In actual implementation, adjust import path as needed
try:
    from Hive_Stored_Procedure_1 import (
        process_sales_data,
        process_sales_data_with_validation,
        process_sales_data_sql_approach
    )
except ImportError:
    # For testing purposes, we'll define mock functions
    # In actual implementation, ensure proper import path
    pass


class TestProcessSalesData:
    """
    Test class for PySpark sales data processing functions
    """
    
    @classmethod
    def setup_class(cls):
        """
        Set up SparkSession for all tests
        """
        cls.spark = SparkSession.builder \n            .appName("TestProcessSalesData") \n            .config("spark.sql.adaptive.enabled", "true") \n            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \n            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \n            .enableHiveSupport() \n            .getOrCreate()
        
        # Set log level to reduce noise during testing
        cls.spark.sparkContext.setLogLevel("WARN")
        
        # Define schema for test data
        cls.sales_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sale_date", DateType(), True),
            StructField("sales", FloatType(), True)
        ])
    
    @classmethod
    def teardown_class(cls):
        """
        Clean up SparkSession after all tests
        """
        cls.spark.stop()
    
    def setup_method(self):
        """
        Set up before each test method
        """
        # Clean up any existing tables/views before each test
        self.cleanup_test_tables()
    
    def teardown_method(self):
        """
        Clean up after each test method
        """
        # Clean up any tables/views created during test
        self.cleanup_test_tables()
    
    def cleanup_test_tables(self):
        """
        Helper method to clean up test tables and views
        """
        tables_to_drop = [
            "sales_table", "summary_table", "detailed_sales_summary"
        ]
        views_to_drop = ["temp_sales_summary"]
        
        for table in tables_to_drop:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass
        
        for view in views_to_drop:
            try:
                self.spark.catalog.dropTempView(view)
            except Exception:
                pass
    
    def create_mock_sales_data(self, data_list):
        """
        Helper method to create mock sales data
        
        Args:
            data_list: List of tuples (product_id, sale_date, sales)
        
        Returns:
            DataFrame with mock sales data
        """
        # Convert string dates to date objects if needed
        processed_data = []
        for product_id, sale_date, sales in data_list:
            if isinstance(sale_date, str):
                sale_date = datetime.strptime(sale_date, '%Y-%m-%d').date()
            processed_data.append((product_id, sale_date, sales))
        
        df = self.spark.createDataFrame(processed_data, self.sales_schema)
        df.createOrReplaceTempView("sales_table")
        return df
    
    def create_output_tables(self):
        """
        Helper method to create output tables for testing
        """
        # Create summary_table
        summary_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("total_sales", FloatType(), True)
        ])
        empty_summary_df = self.spark.createDataFrame([], summary_schema)
        empty_summary_df.write.mode("overwrite").saveAsTable("summary_table")
        
        # Create detailed_sales_summary table
        detailed_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("total_sales", FloatType(), True)
        ])
        empty_detailed_df = self.spark.createDataFrame([], detailed_schema)
        empty_detailed_df.write.mode("overwrite").saveAsTable("detailed_sales_summary")
    
    # GROUP 1: HAPPY PATH SCENARIOS
    
    def test_tc_001_successful_processing_valid_date_range(self):
        """
        TC_001: Test successful processing of sales data with valid date range
        """
        # Arrange
        mock_data = [
            ("P001", "2024-01-15", 100.0),
            ("P002", "2024-01-20", 200.0),
            ("P001", "2024-01-25", 150.0)
        ]
        self.create_mock_sales_data(mock_data)
        self.create_output_tables()
        
        # Act
        process_sales_data("2024-01-01", "2024-01-31")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        detailed_result = self.spark.table("detailed_sales_summary").collect()
        
        assert len(summary_result) == 2
        assert len(detailed_result) == 2
        
        # Check aggregation correctness
        summary_dict = {row.product_id: row.total_sales for row in summary_result}
        assert summary_dict["P001"] == 250.0  # 100 + 150
        assert summary_dict["P002"] == 200.0
    
    def test_tc_002_single_day_date_range(self):
        """
        TC_002: Test processing with single day date range
        """
        # Arrange
        mock_data = [
            ("P001", "2024-01-15", 100.0),
            ("P002", "2024-01-15", 200.0),
            ("P001", "2024-01-16", 150.0)  # Should be excluded
        ]
        self.create_mock_sales_data(mock_data)
        self.create_output_tables()
        
        # Act
        process_sales_data("2024-01-15", "2024-01-15")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        assert len(summary_result) == 2
        
        summary_dict = {row.product_id: row.total_sales for row in summary_result}
        assert summary_dict["P001"] == 100.0
        assert summary_dict["P002"] == 200.0
    
    def test_tc_003_multiple_products_aggregation(self):
        """
        TC_003: Test processing with multiple products and aggregation
        """
        # Arrange
        mock_data = [
            ("P003", "2024-01-15", 300.0),
            ("P001", "2024-01-15", 100.0),
            ("P002", "2024-01-15", 200.0),
            ("P001", "2024-01-20", 150.0),
            ("P003", "2024-01-25", 250.0)
        ]
        self.create_mock_sales_data(mock_data)
        self.create_output_tables()
        
        # Act
        process_sales_data("2024-01-01", "2024-01-31")
        
        # Assert
        summary_result = self.spark.table("summary_table").orderBy("product_id").collect()
        assert len(summary_result) == 3
        
        # Check ordering (should be P001, P002, P003)
        assert summary_result[0].product_id == "P001"
        assert summary_result[1].product_id == "P002"
        assert summary_result[2].product_id == "P003"
        
        # Check aggregation
        assert summary_result[0].total_sales == 250.0  # P001: 100 + 150
        assert summary_result[1].total_sales == 200.0  # P002: 200
        assert summary_result[2].total_sales == 550.0  # P003: 300 + 250
    
    # GROUP 2: EDGE CASES
    
    def test_tc_004_empty_sales_table(self):
        """
        TC_004: Test processing with empty sales table
        """
        # Arrange
        self.create_mock_sales_data([])  # Empty data
        self.create_output_tables()
        
        # Act
        process_sales_data("2024-01-01", "2024-01-31")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        detailed_result = self.spark.table("detailed_sales_summary").collect()
        
        assert len(summary_result) == 0
        assert len(detailed_result) == 0
    
    def test_tc_005_no_records_in_date_range(self):
        """
        TC_005: Test processing with no records in date range
        """
        # Arrange
        mock_data = [
            ("P001", "2024-01-15", 100.0),
            ("P002", "2024-01-20", 200.0)
        ]
        self.create_mock_sales_data(mock_data)
        self.create_output_tables()
        
        # Act - Query for June when data is in January
        process_sales_data("2024-06-01", "2024-06-30")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        detailed_result = self.spark.table("detailed_sales_summary").collect()
        
        assert len(summary_result) == 0
        assert len(detailed_result) == 0
    
    def test_tc_006_null_values_in_sales_data(self):
        """
        TC_006: Test processing with null values in sales data
        """
        # Arrange
        mock_data = [
            ("P001", "2024-01-15", 100.0),
            ("P002", "2024-01-20", None),  # Null sales value
            ("P001", "2024-01-25", 150.0)
        ]
        self.create_mock_sales_data(mock_data)
        self.create_output_tables()
        
        # Act
        process_sales_data("2024-01-01", "2024-01-31")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        summary_dict = {row.product_id: row.total_sales for row in summary_result}
        
        # P001 should have sum of non-null values
        assert summary_dict["P001"] == 250.0
        # P002 should have null or 0 (depending on Spark's sum behavior with nulls)
        assert "P002" not in summary_dict or summary_dict["P002"] is None
    
    def test_tc_007_duplicate_product_records(self):
        """
        TC_007: Test processing with duplicate product records
        """
        # Arrange
        mock_data = [
            ("P001", "2024-01-15", 100.0),
            ("P001", "2024-01-16", 150.0),
            ("P001", "2024-01-17", 200.0)
        ]
        self.create_mock_sales_data(mock_data)
        self.create_output_tables()
        
        # Act
        process_sales_data("2024-01-01", "2024-01-31")
        
        # Assert
        summary_result = self.spark.table("summary_table").collect()
        assert len(summary_result) == 1
        assert summary_result[0].product_id == "P001"
        assert summary_result[0].total_sales == 450.0  # 100 + 150 + 200
    
    def test_tc_008_boundary_date_values(self):\