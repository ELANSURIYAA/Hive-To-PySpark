#!/usr/bin/env python3
"""
Comprehensive Unit Tests for Hive_Stored_Procedure_1.py
PySpark Sales Data Processing Pipeline

This module contains comprehensive unit tests for the PySpark implementation
that converts Hive stored procedure to PySpark DataFrame operations.

Author: Data Engineering Team
Version: 1.0
Date: 2024
"""

import pytest
import sys
from datetime import datetime, date
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import (
        StructType, StructField, StringType, FloatType, 
        DateType, DoubleType, IntegerType
    )
    from pyspark.sql.functions import col, sum as spark_sum, count
except ImportError:
    pytest.skip("PySpark not available", allow_module_level=True)

# Import the module under test
# Assuming the PySpark code is in Hive_Stored_Procedure_1.py
try:
    from Hive_Stored_Procedure_1 import (
        create_spark_session,
        validate_date_format,
        process_sales_data,
        create_sample_data,
        main
    )
except ImportError:
    # Mock imports for testing purposes if module not available
    create_spark_session = Mock()
    validate_date_format = Mock()
    process_sales_data = Mock()
    create_sample_data = Mock()
    main = Mock()


class TestSparkSessionManagement:
    """Test cases for Spark session creation and management."""
    
    def test_create_spark_session_valid_app_name(self):
        """TC_SPARK_001: Test successful Spark session creation with valid app name."""
        app_name = "TestSalesProcessing"
        spark = create_spark_session(app_name)
        
        assert spark is not None
        assert isinstance(spark, SparkSession)
        assert spark.sparkContext.appName == app_name
        
        spark.stop()
    
    def test_create_spark_session_empty_app_name(self):
        """TC_SPARK_002: Test Spark session creation with empty app name."""
        spark = create_spark_session("")
        
        assert spark is not None
        assert isinstance(spark, SparkSession)
        
        spark.stop()
    
    def test_spark_session_configuration(self):
        """TC_SPARK_003: Test Spark session configuration settings."""
        spark = create_spark_session("ConfigTest")
        
        # Verify key configuration settings
        config = spark.sparkContext.getConf()
        
        # Check if adaptive query execution is enabled
        adaptive_enabled = spark.conf.get("spark.sql.adaptive.enabled", "false")
        assert adaptive_enabled.lower() in ["true", "false"]
        
        spark.stop()


class TestDateValidation:
    """Test cases for date format validation functionality."""
    
    def test_valid_date_format_yyyy_mm_dd(self):
        """TC_DATE_001: Test valid date format validation (YYYY-MM-DD)."""
        valid_dates = [
            "2023-01-15",
            "2024-12-31",
            "2022-06-30",
            "2023-02-28"
        ]
        
        for date_str in valid_dates:
            assert validate_date_format(date_str) is True, f"Failed for {date_str}"
    
    def test_invalid_date_format_mm_dd_yyyy(self):
        """TC_DATE_002: Test invalid date format validation (MM/DD/YYYY)."""
        invalid_dates = [
            "01/15/2023",
            "12/31/2024",
            "06/30/2022"
        ]
        
        for date_str in invalid_dates:
            assert validate_date_format(date_str) is False, f"Should fail for {date_str}"
    
    def test_invalid_date_format_dd_mm_yyyy(self):
        """TC_DATE_003: Test invalid date format validation (DD-MM-YYYY)."""
        invalid_dates = [
            "15-01-2023",
            "31-12-2024",
            "30-06-2022"
        ]
        
        for date_str in invalid_dates:
            assert validate_date_format(date_str) is False, f"Should fail for {date_str}"
    
    def test_empty_string_date_validation(self):
        """TC_DATE_004: Test empty string date validation."""
        assert validate_date_format("") is False
    
    def test_none_value_date_validation(self):
        """TC_DATE_005: Test None value date validation."""
        assert validate_date_format(None) is False
    
    def test_invalid_date_values(self):
        """TC_DATE_006: Test invalid date values (February 30th)."""
        invalid_dates = [
            "2023-02-30",
            "2023-04-31",
            "2023-13-01",
            "2023-00-15"
        ]
        
        for date_str in invalid_dates:
            assert validate_date_format(date_str) is False, f"Should fail for {date_str}"
    
    def test_leap_year_date_validation(self):
        """TC_DATE_007: Test leap year date validation."""
        # Valid leap year dates
        assert validate_date_format("2024-02-29") is True
        assert validate_date_format("2020-02-29") is True
        
        # Invalid leap year dates
        assert validate_date_format("2023-02-29") is False
        assert validate_date_format("2021-02-29") is False


class TestSampleDataCreation:
    """Test cases for sample data creation functionality."""
    
    @pytest.fixture
    def spark_session(self):
        """Create Spark session for testing."""
        spark = SparkSession.builder \
            .appName("TestSampleData") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_sample_data_creation_valid_spark_session(self, spark_session):
        """TC_SAMPLE_001: Test sample data creation with valid Spark session."""
        create_sample_data(spark_session)
        
        # Verify table was created
        tables = spark_session.catalog.listTables()
        table_names = [table.name for table in tables]
        assert "sales_table" in table_names
        
        # Verify DataFrame exists
        df = spark_session.table("sales_table")
        assert df is not None
        assert isinstance(df, DataFrame)
    
    def test_sample_data_content_validation(self, spark_session):
        """TC_SAMPLE_002: Test sample data content validation."""
        create_sample_data(spark_session)
        df = spark_session.table("sales_table")
        
        # Check schema
        expected_columns = {"product_id", "sales", "sale_date"}
        actual_columns = set(df.columns)
        assert expected_columns == actual_columns
        
        # Check data types
        schema = df.schema
        field_types = {field.name: field.dataType for field in schema.fields}
        
        assert isinstance(field_types["product_id"], StringType)
        assert isinstance(field_types["sales"], FloatType)
        assert isinstance(field_types["sale_date"], DateType)
        
        # Check row count
        row_count = df.count()
        assert row_count > 0
    
    def test_sample_data_date_range_coverage(self, spark_session):
        """TC_SAMPLE_003: Test sample data date range coverage."""
        create_sample_data(spark_session)
        df = spark_session.table("sales_table")
        
        # Get date range
        date_stats = df.agg(
            {"sale_date": "min", "sale_date": "max"}
        ).collect()[0]
        
        min_date = date_stats[0]
        max_date = date_stats[1]
        
        assert min_date is not None
        assert max_date is not None
        assert min_date <= max_date


class TestMainDataProcessing:
    """Test cases for main data processing functionality."""
    
    @pytest.fixture
    def spark_session(self):
        """Create Spark session for testing."""
        spark = SparkSession.builder \
            .appName("TestDataProcessing") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_sales_data(self, spark_session):
        """Create sample sales data for testing."""
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        data = [
            ("PROD001", 1500.50, date(2024, 1, 15)),
            ("PROD002", 2300.75, date(2024, 1, 16)),
            ("PROD001", 1200.25, date(2024, 1, 17)),
            ("PROD003", 3400.00, date(2024, 1, 18)),
            ("PROD002", 1800.30, date(2024, 1, 19)),
            ("PROD001", 2100.80, date(2024, 1, 20)),
            ("PROD004", 950.40, date(2024, 1, 21)),
            ("PROD003", 2750.60, date(2024, 1, 22)),
            ("PROD002", 1650.90, date(2024, 1, 23)),
            ("PROD004", 1100.20, date(2024, 1, 24))
        ]
        
        df = spark_session.createDataFrame(data, schema)
        df.createOrReplaceTempView("sales_table")
        return df
    
    def test_successful_sales_data_processing(self, spark_session, sample_sales_data):
        """TC_PROCESS_001: Test successful sales data processing with valid date range."""
        # Process data for a specific date range
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-16",
            end_date="2024-01-20"
        )
        
        # Verify summary_table was created
        summary_df = spark_session.table("summary_table")
        assert summary_df.count() > 0
        
        # Verify detailed_sales_summary was created
        detailed_df = spark_session.table("detailed_sales_summary")
        assert detailed_df.count() > 0
    
    def test_single_day_processing(self, spark_session, sample_sales_data):
        """TC_PROCESS_002: Test data processing with start_date equals end_date."""
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-16",
            end_date="2024-01-16"
        )
        
        summary_df = spark_session.table("summary_table")
        rows = summary_df.collect()
        
        # Should have only one product for that specific date
        assert len(rows) == 1
        assert rows[0]["product_id"] == "PROD002"
    
    def test_wide_date_range_processing(self, spark_session, sample_sales_data):
        """TC_PROCESS_003: Test data processing with wide date range."""
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-15",
            end_date="2024-01-24"
        )
        
        summary_df = spark_session.table("summary_table")
        product_count = summary_df.count()
        
        # Should have all 4 products
        assert product_count == 4


class TestDataFilteringAndAggregation:
    """Test cases for data filtering and aggregation functionality."""
    
    @pytest.fixture
    def spark_session(self):
        """Create Spark session for testing."""
        spark = SparkSession.builder \
            .appName("TestFiltering") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def test_data(self, spark_session):
        """Create test data for filtering and aggregation tests."""
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        data = [
            ("PROD001", 1000.0, date(2024, 1, 15)),
            ("PROD001", 1500.0, date(2024, 1, 16)),
            ("PROD002", 2000.0, date(2024, 1, 17)),
            ("PROD002", 2500.0, date(2024, 1, 18)),
            ("PROD003", 3000.0, date(2024, 1, 19))
        ]
        
        df = spark_session.createDataFrame(data, schema)
        df.createOrReplaceTempView("sales_table")
        return df
    
    def test_date_range_filtering(self, spark_session, test_data):
        """TC_FILTER_001: Test date range filtering functionality."""
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-16",
            end_date="2024-01-18"
        )
        
        summary_df = spark_session.table("summary_table")
        results = summary_df.collect()
        
        # Should only have PROD001 and PROD002
        product_ids = {row["product_id"] for row in results}
        expected_products = {"PROD001", "PROD002"}
        assert product_ids == expected_products
    
    def test_groupby_product_id(self, spark_session, test_data):
        """TC_FILTER_002: Test groupBy product_id functionality."""
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-15",
            end_date="2024-01-19"
        )
        
        summary_df = spark_session.table("summary_table")
        results = summary_df.collect()
        
        # Check no duplicate product_ids
        product_ids = [row["product_id"] for row in results]
        assert len(product_ids) == len(set(product_ids))
    
    def test_sales_sum_aggregation_accuracy(self, spark_session, test_data):
        """TC_AGG_001: Test sales sum aggregation accuracy."""
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-15",
            end_date="2024-01-19"
        )
        
        summary_df = spark_session.table("summary_table")
        results = {row["product_id"]: row["total_sales"] for row in summary_df.collect()}
        
        # Verify aggregated values
        assert abs(results["PROD001"] - 2500.0) < 0.01  # 1000 + 1500
        assert abs(results["PROD002"] - 4500.0) < 0.01  # 2000 + 2500
        assert abs(results["PROD003"] - 3000.0) < 0.01  # 3000


class TestEdgeCases:
    """Test cases for edge case scenarios."""
    
    @pytest.fixture
    def spark_session(self):
        """Create Spark session for testing."""
        spark = SparkSession.builder \
            .appName("TestEdgeCases") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_empty_dataframe_processing(self, spark_session):
        """TC_EDGE_001: Test processing with empty DataFrame."""
        # Create empty DataFrame with correct schema
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        empty_df = spark_session.createDataFrame([], schema)
        empty_df.createOrReplaceTempView("sales_table")
        
        # Should not raise exception
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-15",
            end_date="2024-01-20"
        )
        
        summary_df = spark_session.table("summary_table")
        assert summary_df.count() == 0
    
    def test_no_records_in_date_range(self, spark_session):
        """TC_EDGE_002: Test processing with no records in date range."""
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        data = [("PROD001", 1000.0, date(2024, 2, 15))]
        df = spark_session.createDataFrame(data, schema)
        df.createOrReplaceTempView("sales_table")
        
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-15",
            end_date="2024-01-20"
        )
        
        summary_df = spark_session.table("summary_table")
        assert summary_df.count() == 0
    
    def test_single_record_processing(self, spark_session):
        """TC_EDGE_003: Test processing with single record."""
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        data = [("PROD001", 1000.0, date(2024, 1, 15))]
        df = spark_session.createDataFrame(data, schema)
        df.createOrReplaceTempView("sales_table")
        
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-15",
            end_date="2024-01-15"
        )
        
        summary_df = spark_session.table("summary_table")
        results = summary_df.collect()
        
        assert len(results) == 1
        assert results[0]["product_id"] == "PROD001"
        assert abs(results[0]["total_sales"] - 1000.0) < 0.01


class TestErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.fixture
    def spark_session(self):
        """Create Spark session for testing."""
        spark = SparkSession.builder \
            .appName("TestErrorHandling") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_invalid_start_date_format(self, spark_session):
        """TC_ERROR_001: Test processing with invalid start_date format."""
        with pytest.raises(ValueError):
            process_sales_data(
                spark=spark_session,
                start_date="01/15/2024",  # Invalid format
                end_date="2024-01-20"
            )
    
    def test_invalid_end_date_format(self, spark_session):
        """TC_ERROR_002: Test processing with invalid end_date format."""
        with pytest.raises(ValueError):
            process_sales_data(
                spark=spark_session,
                start_date="2024-01-15",
                end_date="20/01/2024"  # Invalid format
            )
    
    def test_start_date_greater_than_end_date(self, spark_session):
        """TC_ERROR_003: Test processing with start_date > end_date."""
        # Create sample data
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        data = [("PROD001", 1000.0, date(2024, 1, 15))]
        df = spark_session.createDataFrame(data, schema)
        df.createOrReplaceTempView("sales_table")
        
        # This should result in empty DataFrame but not crash
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-20",
            end_date="2024-01-15"
        )
        
        summary_df = spark_session.table("summary_table")
        assert summary_df.count() == 0


class TestDataValidation:
    """Test cases for data validation scenarios."""
    
    @pytest.fixture
    def spark_session(self):
        """Create Spark session for testing."""
        spark = SparkSession.builder \
            .appName("TestDataValidation") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def validation_data(self, spark_session):
        """Create validation test data."""
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        data = [
            ("PROD001", 1000.0, date(2024, 1, 15)),
            ("PROD002", 2000.0, date(2024, 1, 16)),
            ("PROD001", 1500.0, date(2024, 1, 17))
        ]
        
        df = spark_session.createDataFrame(data, schema)
        df.createOrReplaceTempView("sales_table")
        return df
    
    def test_output_dataframe_schema_validation(self, spark_session, validation_data):
        """TC_VALID_001: Test output DataFrame schema validation."""
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-15",
            end_date="2024-01-17"
        )
        
        summary_df = spark_session.table("summary_table")
        schema = summary_df.schema
        
        # Check expected columns
        column_names = [field.name for field in schema.fields]
        assert "product_id" in column_names
        assert "total_sales" in column_names
        
        # Check data types
        field_types = {field.name: field.dataType for field in schema.fields}
        assert isinstance(field_types["product_id"], StringType)
        assert isinstance(field_types["total_sales"], (FloatType, DoubleType))
    
    def test_output_data_completeness(self, spark_session, validation_data):
        """TC_VALID_002: Test output data completeness."""
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-15",
            end_date="2024-01-17"
        )
        
        summary_df = spark_session.table("summary_table")
        results = summary_df.collect()
        
        product_ids = {row["product_id"] for row in results}
        expected_products = {"PROD001", "PROD002"}
        assert product_ids == expected_products
    
    def test_no_duplicate_product_ids_in_output(self, spark_session, validation_data):
        """TC_VALID_004: Test no duplicate product_ids in output."""
        process_sales_data(
            spark=spark_session,
            start_date="2024-01-15",
            end_date="2024-01-17"
        )
        
        summary_df = spark_session.table("summary_table")
        results = summary_df.collect()
        
        product_ids = [row["product_id"] for row in results]
        assert len(product_ids) == len(set(product_ids))


class TestIntegration:
    """Integration test cases for end-to-end functionality."""
    
    def test_main_function_execution(self):
        """TC_INTEG_002: Test main() function execution."""
        # Mock the main function to avoid actual Spark session creation
        with patch('Hive_Stored_Procedure_1.create_spark_session') as mock_spark:
            with patch('Hive_Stored_Procedure_1.create_sample_data'):
                with patch('Hive_Stored_Procedure_1.process_sales_data'):
                    mock_spark_instance = Mock()
                    mock_spark.return_value = mock_spark_instance
                    
                    # Should not raise exception
                    try:
                        main()
                    except Exception as e:
                        pytest.fail(f"main() function failed: {e}")


# Test configuration and fixtures
@pytest.fixture(scope="session")
def spark_test_session():
    """Create a Spark session for the entire test session."""
    spark = SparkSession.builder \
        .appName("HiveToSparkUnitTests") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    yield spark
    spark.stop()


# Performance test markers
pytestmark = pytest.mark.spark


if __name__ == "__main__":
    # Run tests with coverage
    pytest.main([
        __file__,
        "-v",
        "--tb=short",
        "--cov=Hive_Stored_Procedure_1",
        "--cov-report=html",
        "--cov-report=term-missing"
    ])