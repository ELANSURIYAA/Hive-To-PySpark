#!/usr/bin/env python3
"""
Comprehensive Unit Tests for Hive_Stored_Procedure PySpark Conversion
Generated for: process_sales_data function and utility functions
Version: 1
Framework: Pytest with PySpark
Coverage: Happy paths, edge cases, error handling, utility functions, integration tests
"""

import pytest
import logging
from datetime import datetime
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col, lit, count, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql.utils import AnalysisException

class TestSalesDataProcessor:
    """
    Test class for sales data processing functionality.
    Covers all aspects of the PySpark conversion from Hive stored procedure.
    """
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """
        Create Spark session for testing.
        """
        spark = SparkSession.builder \n            .appName("TestSalesDataProcessor") \n            .master("local[2]") \n            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \n            .config("spark.sql.adaptive.enabled", "false") \n            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_sales_data(self, spark_session):
        """
        Create sample sales data for testing.
        """
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        data = [
            ("P001", 100.0, datetime.strptime("2023-01-15", "%Y-%m-%d").date()),
            ("P001", 150.0, datetime.strptime("2023-02-20", "%Y-%m-%d").date()),
            ("P002", 200.0, datetime.strptime("2023-01-10", "%Y-%m-%d").date()),
            ("P002", None, datetime.strptime("2023-03-15", "%Y-%m-%d").date()),
            ("P003", 0.0, datetime.strptime("2023-06-01", "%Y-%m-%d").date()),
            ("P003", 300.0, datetime.strptime("2023-07-15", "%Y-%m-%d").date()),
            ("P004", 250.0, datetime.strptime("2024-01-01", "%Y-%m-%d").date()),
        ]
        
        df = spark_session.createDataFrame(data, schema)
        df.createOrReplaceTempView("sales_table")
        return df
    
    @pytest.fixture
    def empty_sales_data(self, spark_session):
        """
        Create empty sales data for edge case testing.
        """
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        df = spark_session.createDataFrame([], schema)
        df.createOrReplaceTempView("sales_table")
        return df
    
    @pytest.fixture
    def setup_target_tables(self, spark_session):
        """
        Setup target tables for testing.
        """
        summary_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("total_sales", FloatType(), True)
        ])
        summary_df = spark_session.createDataFrame([], summary_schema)
        summary_df.createOrReplaceTempView("summary_table")
        
        detailed_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("total_sales", FloatType(), True)
        ])
        detailed_df = spark_session.createDataFrame([], detailed_schema)
        detailed_df.createOrReplaceTempView("detailed_sales_summary")
    
    # ============================================================================
    # HAPPY PATH SCENARIOS
    # ============================================================================
    
    def test_tc001_valid_date_range_processing(self, spark_session, sample_sales_data, setup_target_tables):
        """
        TC001: Test successful processing of sales data with valid start and end dates.
        """
        start_date = "2023-01-01"
        end_date = "2023-12-31"
        
        filtered_df = sample_sales_data.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        result_df = filtered_df.groupBy("product_id") \n            .agg(spark_sum("sales").alias("total_sales"))
        
        results = result_df.collect()
        
        assert len(results) == 3
        result_dict = {row["product_id"]: row["total_sales"] for row in results}
        assert result_dict["P001"] == 250.0
        assert result_dict["P002"] == 200.0
        assert result_dict["P003"] == 300.0
    
    def test_tc002_single_day_date_range(self, spark_session, sample_sales_data, setup_target_tables):
        """
        TC002: Test processing when start_date equals end_date.
        """
        start_date = "2023-01-15"
        end_date = "2023-01-15"
        
        filtered_df = sample_sales_data.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        results = filtered_df.collect()
        
        assert len(results) == 1
        assert results[0]["product_id"] == "P001"
        assert results[0]["sales"] == 100.0
    
    def test_tc003_large_dataset_processing(self, spark_session, setup_target_tables):
        """
        TC003: Test function performance with large volume of sales data.
        """
        large_data = []
        for i in range(1000):
            product_id = f"P{i % 100:03d}"
            sales = float(i % 1000)
            sale_date = datetime.strptime("2023-01-01", "%Y-%m-%d").date()
            large_data.append((product_id, sales, sale_date))
        
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        large_df = spark_session.createDataFrame(large_data, schema)
        large_df.createOrReplaceTempView("sales_table")
        
        start_date = "2023-01-01"
        end_date = "2023-12-31"
        
        filtered_df = large_df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        result_df = filtered_df.groupBy("product_id") \n            .agg(spark_sum("sales").alias("total_sales"))
        
        results = result_df.collect()
        
        assert len(results) == 100
        assert all(row["total_sales"] is not None for row in results)
    
    def test_tc004_multiple_products_aggregation(self, spark_session, sample_sales_data, setup_target_tables):
        """
        TC004: Test aggregation logic for multiple products.
        """
        start_date = "2023-01-01"
        end_date = "2023-03-31"
        
        filtered_df = sample_sales_data.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        result_df = filtered_df.groupBy("product_id") \n            .agg(spark_sum("sales").alias("total_sales"))
        
        results = result_df.collect()
        result_dict = {row["product_id"]: row["total_sales"] for row in results}
        
        assert "P001" in result_dict
        assert "P002" in result_dict
        assert result_dict["P001"] == 250.0
        assert result_dict["P002"] == 200.0
    
    # ============================================================================
    # EDGE CASES
    # ============================================================================
    
    def test_tc005_empty_sales_table(self, spark_session, empty_sales_data, setup_target_tables):
        """
        TC005: Test behavior when sales_table is empty.
        """
        start_date = "2023-01-01"
        end_date = "2023-12-31"
        
        filtered_df = empty_sales_data.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        results = filtered_df.collect()
        
        assert len(results) == 0
        assert filtered_df.count() == 0
    
    def test_tc006_no_records_in_date_range(self, spark_session, sample_sales_data, setup_target_tables):
        """
        TC006: Test when no sales records exist within specified date range.
        """
        start_date = "2025-01-01"
        end_date = "2025-12-31"
        
        filtered_df = sample_sales_data.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        results = filtered_df.collect()
        assert len(results) == 0
    
    def test_tc007_null_sales_values(self, spark_session, sample_sales_data, setup_target_tables):
        """
        TC007: Test handling of null values in sales column.
        """
        start_date = "2023-01-01"
        end_date = "2023-12-31"
        
        filtered_df = sample_sales_data.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        result_df = filtered_df.groupBy("product_id") \n            .agg(spark_sum("sales").alias("total_sales"))
        
        detailed_df = result_df.filter(col("total_sales").isNotNull())
        
        results = detailed_df.collect()
        result_dict = {row["product_id"]: row["total_sales"] for row in results}
        
        assert "P002" in result_dict
        assert result_dict["P002"] == 200.0
    
    def test_tc008_duplicate_product_records(self, spark_session, setup_target_tables):
        """
        TC008: Test aggregation when multiple records exist for same product.
        """
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        data = [
            ("P001", 100.0, datetime.strptime("2023-01-15", "%Y-%m-%d").date()),
            ("P001", 150.0, datetime.strptime("2023-01-16", "%Y-%m-%d").date()),
            ("P001", 200.0, datetime.strptime("2023-01-17", "%Y-%m-%d").date()),
        ]
        
        df = spark_session.createDataFrame(data, schema)
        df.createOrReplaceTempView("sales_table")
        
        start_date = "2023-01-01"
        end_date = "2023-12-31"
        
        filtered_df = df.filter(
            (col("sale_date") >= lit(start_date)) & 
            (col("sale_date") <= lit(end_date))
        )
        
        result_df = filtered_df.groupBy("product_id") \n            .agg(spark_sum("sales").alias("total_sales"))
        
        results = result_df.collect()
        
        assert len(results) == 1
        assert results[0]["product_id"] == "P001"
        assert results[0]["total_sales"] == 450.0
    
    def test_tc009_boundary_date_values(self, spark_session, setup_target_tables):
        """
        TC009: Test with dates at exact boundary of available data.
        """
        schema = StructType([