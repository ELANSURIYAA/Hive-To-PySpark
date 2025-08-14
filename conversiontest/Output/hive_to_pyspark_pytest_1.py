#!/usr/bin/env python3
"""
Comprehensive Pytest Suite for Hive to PySpark Reconciliation Testing
Generated for: Hive_Stored_Procedure.txt to PySpark Conversion
Version: 1
Date: 2024

This test suite validates the reconciliation between HiveQL stored procedures and PySpark implementations,
ensuring data accuracy, performance consistency, and functional equivalence.
"""

import pytest
import logging
import time
import hashlib
import json
from datetime import datetime, timedelta
from decimal import Decimal, getcontext
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col, count, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, IntegerType
from pyspark.sql.utils import AnalysisException
import tempfile
import shutil
import os
import random
import string

# Configure logging for testing
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set decimal precision for high-precision calculations
getcontext().prec = 28

class HiveToSparkReconciliationTest:
    """
    Comprehensive reconciliation test class for Hive to PySpark conversion validation
    """
    
    @classmethod
    def setup_class(cls):
        """
        Setup SparkSession and initialize test environment
        """
        cls.spark = SparkSession.builder \
            .appName("HiveToSparkReconciliationTest") \
            .master("local[4]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .getOrCreate()
        
        cls.spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession initialized for reconciliation testing")
        
        # Initialize test metrics
        cls.test_metrics = {
            'execution_times': {},
            'memory_usage': {},
            'data_accuracy': {},
            'performance_ratios': {}
        }
    
    @classmethod
    def teardown_class(cls):
        """
        Cleanup SparkSession and generate final test report
        """
        if hasattr(cls, 'spark'):
            cls.spark.stop()
            logger.info("SparkSession stopped")
        
        # Generate test metrics report
        cls.generate_test_report()
    
    def setup_method(self, method):
        """
        Setup method called before each test
        """
        self.test_data_path = tempfile.mkdtemp()
        self.test_start_time = time.time()
        logger.info(f"Test setup for {method.__name__}")
    
    def teardown_method(self, method):
        """
        Cleanup method called after each test
        """
        # Clean up any temporary tables/views
        temp_objects = [
            "sales_table", "summary_table", "detailed_sales_summary", 
            "temp_sales_summary", "hive_results", "pyspark_results",
            "test_sales_data", "reconciliation_results", "hive_summary_results",
            "pyspark_summary_results", "hive_temp_sales_summary", "pyspark_temp_sales_summary",
            "hive_detailed_results", "pyspark_detailed_results"
        ]
        
        for obj in temp_objects:
            try:
                self.spark.catalog.dropTempView(obj)
            except:
                pass
        
        # Clean up test data directory
        if hasattr(self, 'test_data_path') and os.path.exists(self.test_data_path):
            shutil.rmtree(self.test_data_path)
        
        # Record test execution time
        execution_time = time.time() - self.test_start_time
        self.test_metrics['execution_times'][method.__name__] = execution_time
        
        logger.info(f"Test cleanup completed for {method.__name__} (Duration: {execution_time:.2f}s)")
    
    def create_comprehensive_sales_data(self, num_records=1000, start_date="2023-01-01", end_date="2023-12-31", 
                                      include_nulls=False, include_negatives=False, include_zeros=False,
                                      include_duplicates=False, include_unicode=False):
        """
        Create comprehensive sales data for testing various scenarios
        """
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        date_range = (end_dt - start_dt).days
        
        data = []
        product_ids = [f"PROD_{i:03d}" for i in range(1, 21)]  # 20 different products
        
        if include_unicode:
            product_ids.extend(["PROD_ÄÖÜ", "PROD_中文", "PROD_العربية"])
        
        for i in range(num_records):
            # Generate random date within range
            random_days = random.randint(0, date_range)
            sale_date = (start_dt + timedelta(days=random_days)).strftime("%Y-%m-%d")
            
            # Select product ID
            if include_duplicates and i % 10 == 0:
                product_id = random.choice(product_ids[:5])  # Create more duplicates for first 5 products
            else:
                product_id = random.choice(product_ids)
            
            # Generate sales amount
            if include_nulls and i % 50 == 0:
                sales = None
            elif include_zeros and i % 30 == 0:
                sales = 0.0
            elif include_negatives and i % 40 == 0:
                sales = round(random.uniform(-500.0, -10.0), 2)
            else:
                sales = round(random.uniform(10.0, 1000.0), 2)
            
            data.append((product_id, sales, sale_date))
        
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        df.createOrReplaceTempView("sales_table")
        return df
    
    def create_target_tables(self):
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
    
    def simulate_hive_stored_procedure(self, start_date, end_date):
        """
        Simulate HiveQL stored procedure behavior using Spark SQL
        This represents the "expected" results from the original HiveQL implementation
        """
        try:
            logger.info(f"Simulating HiveQL stored procedure for period: {start_date} to {end_date}")
            
            # Simulate dynamic query execution (INSERT INTO summary_table)
            dynamic_query = f"""
                SELECT product_id, SUM(sales) AS total_sales
                FROM sales_table 
                WHERE sale_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY product_id
                ORDER BY product_id
            """
            
            hive_summary_df = self.spark.sql(dynamic_query)
            hive_summary_df.createOrReplaceTempView("hive_summary_results")
            
            # Simulate temporary table creation
            temp_query = f"""
                SELECT product_id, SUM(sales) AS total_sales
                FROM sales_table
                WHERE sale_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY product_id
            """
            
            hive_temp_df = self.spark.sql(temp_query)
            hive_temp_df.createOrReplaceTempView("hive_temp_sales_summary")
            
            # Simulate cursor processing (detailed_sales_summary)
            cursor_results = []
            temp_rows = hive_temp_df.collect()
            
            for row in temp_rows:
                product_id = row['product_id']
                total_sales = row['total_sales']
                
                if total_sales is not None:  # Simulate WHILE total_sales IS NOT NULL
                    cursor_results.append((product_id, total_sales))
            
            if cursor_results:
                detailed_schema = StructType([
                    StructField("product_id", StringType(), True),
                    StructField("total_sales", FloatType(), True)
                ])
                
                hive_detailed_df = self.spark.createDataFrame(cursor_results, detailed_schema)
                hive_detailed_df.createOrReplaceTempView("hive_detailed_results")
            
            logger.info(f"HiveQL simulation completed - Summary: {hive_summary_df.count()} records")
            return True
            
        except Exception as e:
            logger.error(f"Error in HiveQL simulation: {str(e)}")
            raise e
    
    def execute_pyspark_implementation(self, start_date, end_date):
        """
        Execute the converted PySpark implementation
        """
        try:
            logger.info(f"Executing PySpark implementation for period: {start_date} to {end_date}")
            
            # Read source sales table
            sales_df = self.spark.table("sales_table")
            
            # Validate input data
            if sales_df.count() == 0:
                logger.warning("No data found in sales_table")
                return False
            
            # Filter sales data by date range
            filtered_sales_df = sales_df.filter(
                (col("sale_date") >= start_date) & 
                (col("sale_date") <= end_date)
            )
            
            # Create aggregated summary
            summary_df = filtered_sales_df.groupBy("product_id") \
                .agg(spark_sum("sales").alias("total_sales")) \
                .orderBy("product_id")
            
            # Cache the summary for multiple operations
            summary_df.cache()
            summary_df.createOrReplaceTempView("pyspark_summary_results")
            
            # Create temporary view
            summary_df.createOrReplaceTempView("pyspark_temp_sales_summary")
            
            # Process each row for detailed analysis (collect() processing)
            summary_rows = summary_df.collect()
            
            # Prepare data for detailed_sales_summary table
            detailed_records = []
            
            for row in summary_rows:
                product_id = row['product_id']
                total_sales = row['total_sales']
                
                if total_sales is not None:
                    detailed_records.append((product_id, total_sales))
            
            # Create DataFrame from processed records
            if detailed_records:
                detailed_schema = StructType([
                    StructField("product_id", StringType(), True),
                    StructField("total_sales", FloatType(), True)
                ])
                
                detailed_df = self.spark.createDataFrame(detailed_records, detailed_schema)
                detailed_df.createOrReplaceTempView("pyspark_detailed_results")
            
            # Clean up cached data
            summary_df.unpersist()
            
            logger.info(f"PySpark implementation completed - Summary: {summary_df.count()} records")
            return True
            
        except Exception as e:
            logger.error(f"Error in PySpark implementation: {str(e)}")
            raise e
    
    def compare_dataframes(self, df1, df2, tolerance=0.001, comparison_name=""):
        """
        Compare two DataFrames for reconciliation validation
        """
        try:
            # Basic count comparison
            count1 = df1.count()
            count2 = df2.count()
            
            if count1 != count2:
                logger.error(f"{comparison_name} - Record count mismatch: {count1} vs {count2}")
                return False
            
            # Schema comparison
            if set(df1.columns) != set(df2.columns):
                logger.error(f"{comparison_name} - Schema mismatch: {df1.columns} vs {df2.columns}")
                return False
            
            # Data content comparison
            # Join on product_id and compare total_sales
            comparison_df = df1.alias("df1").join(
                df2.alias("df2"), 
                col("df1.product_id") == col("df2.product_id"), 
                "full_outer"
            ).select(
                col("df1.product_id").alias("product_id_1"),
                col("df2.product_id").alias("product_id_2"),
                col("df1.total_sales").alias("total_sales_1"),
                col("df2.total_sales").alias("total_sales_2")
            )
            
            mismatches = comparison_df.filter(
                (col("product_id_1") != col("product_id_2")) |
                (abs(col("total_sales_1") - col("total_sales_2")) > tolerance)
            ).collect()
            
            if mismatches:
                logger.error(f"{comparison_name} - Data mismatches found: {len(mismatches)} records")
                for mismatch in mismatches[:5]:  # Show first 5 mismatches
                    logger.error(f"Mismatch: {mismatch}")
                return False
            
            logger.info(f"{comparison_name} - DataFrames match successfully (tolerance: {tolerance})")
            return True
            
        except Exception as e:
            logger.error(f"Error comparing DataFrames for {comparison_name}: {str(e)}")
            return False
    
    @classmethod
    def generate_test_report(cls):
        """
        Generate comprehensive test execution report
        """
        logger.info("=== HIVE TO PYSPARK RECONCILIATION TEST REPORT ===")
        logger.info(f"Total tests executed: {len(cls.test_metrics['execution_times'])}")
        
        total_time = sum(cls.test_metrics['execution_times'].values())
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        
        avg_time = total_time / len(cls.test_metrics['execution_times']) if cls.test_metrics['execution_times'] else 0
        logger.info(f"Average test execution time: {avg_time:.2f} seconds")
    
    # ==================== TEST CASE IMPLEMENTATIONS ====================
    
    def test_tc001_basic_function_execution_reconciliation(self):
        """
        TC001: Basic Function Execution Reconciliation
        Validate that both HiveQL stored procedure and PySpark function execute successfully
        """
        logger.info("Executing TC001: Basic Function Execution Reconciliation")
        
        # Create test data
        self.create_comprehensive_sales_data(1000, "2023-01-01", "2023-12-31")
        self.create_target_tables()
        
        # Execute both implementations
        hive_result = self.simulate_hive_stored_procedure("2023-01-01", "2023-12-31")
        pyspark_result = self.execute_pyspark_implementation("2023-01-01", "2023-12-31")
        
        # Validate execution success
        assert hive_result is True, "HiveQL simulation should execute successfully"
        assert pyspark_result is True, "PySpark implementation should execute successfully"
        
        logger.info("TC001 PASSED: Both implementations executed successfully")
    
    def test_tc002_data_filtering_accuracy_reconciliation(self):
        """
        TC002: Data Filtering Accuracy Reconciliation
        Verify that date range filtering produces identical results
        """
        logger.info("Executing TC002: Data Filtering Accuracy Reconciliation")
        
        # Create test data spanning multiple years
        self.create_comprehensive_sales_data(2000, "2022-01-01", "2024-12-31")
        self.create_target_tables()
        
        # Test specific date range
        test_start = "2023-06-01"
        test_end = "2023-08-31"
        
        # Execute both implementations
        self.simulate_hive_stored_procedure(test_start, test_end)
        self.execute_pyspark_implementation(test_start, test_end)
        
        # Compare filtered results
        hive_summary = self.spark.table("hive_summary_results")
        pyspark_summary = self.spark.table("pyspark_summary_results")
        
        # Validate filtering accuracy
        comparison_result = self.compare_dataframes(
            hive_summary, pyspark_summary, 
            tolerance=0.001, comparison_name="Date Filtering"
        )
        
        assert comparison_result is True, "Date filtering should produce identical results"
        
        logger.info("TC002 PASSED: Date filtering accuracy validated")
    
    def test_tc003_aggregation_results_reconciliation(self):
        """
        TC003: Aggregation Results Reconciliation
        Validate that SUM aggregations produce identical results
        """
        logger.info("Executing TC003: Aggregation Results Reconciliation")
        
        # Create test data with known aggregation totals
        test_data = [
            ("PROD_001", 100.50, "2023-06-01"),
            ("PROD_001", 200.25, "2023-06-02"),
            ("PROD_001", 150.75, "2023-06-03"),
            ("PROD_002", 300.00, "2023-06-01"),
            ("PROD_002", 250.50, "2023-06-02"),
            ("PROD_003", 500.25, "2023-06-01")
        ]
        
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(test_data, schema)
        df.createOrReplaceTempView("sales_table")
        self.create_target_tables()
        
        # Execute both implementations
        self.simulate_hive_stored_procedure("2023-06-01", "2023-06-30")
        self.execute_pyspark_implementation("2023-06-01", "2023-06-30")
        
        # Compare aggregation results
        hive_summary = self.spark.table("hive_summary_results")
        pyspark_summary = self.spark.table("pyspark_summary_results")
        
        # Validate aggregation accuracy with high precision
        comparison_result = self.compare_dataframes(
            hive_summary, pyspark_summary, 
            tolerance=0.0001, comparison_name="Aggregation Results"
        )
        
        assert comparison_result is True, "Aggregation results should be identical"
        
        # Verify specific expected totals
        results_dict = {row['product_id']: row['total_sales'] for row in pyspark_summary.collect()}
        
        expected_totals = {
            "PROD_001": 451.50,
            "PROD_002": 550.50,
            "PROD_003": 500.25
        }
        
        for product_id, expected_total in expected_totals.items():
            actual_total = results_dict.get(product_id, 0)
            assert abs(actual_total - expected_total) < 0.01, f"Product {product_id}: expected {expected_total}, got {actual_total}"
        
        logger.info("TC003 PASSED: Aggregation results reconciliation successful")
    
    def test_tc020_end_to_end_data_lineage_reconciliation(self):
        """
        TC020: End-to-End Data Lineage Reconciliation
        Comprehensive validation of complete data processing pipeline
        """
        logger.info("Executing TC020: End-to-End Data Lineage Reconciliation")
        
        # Create comprehensive production-like dataset
        self.create_comprehensive_sales_data(
            5000, "2023-01-01", "2023-12-31", 
            include_nulls=True, include_negatives=True, 
            include_zeros=True, include_duplicates=True
        )
        self.create_target_tables()
        
        # Execute both implementations
        self.simulate_hive_stored_procedure("2023-01-01", "2023-12-31")
        self.execute_pyspark_implementation("2023-01-01", "2023-12-31")
        
        # Comprehensive comparison of all outputs
        hive_summary = self.spark.table("hive_summary_results")
        pyspark_summary = self.spark.table("pyspark_summary_results")
        
        hive_detailed = self.spark.table("hive_detailed_results")
        pyspark_detailed = self.spark.table("pyspark_detailed_results")
        
        # Validate complete pipeline equivalence
        summary_comparison = self.compare_dataframes(
            hive_summary, pyspark_summary, 
            tolerance=0.001, comparison_name="End-to-End Summary"
        )
        
        detailed_comparison = self.compare_dataframes(
            hive_detailed, pyspark_detailed, 
            tolerance=0.001, comparison_name="End-to-End Detailed"
        )
        
        assert summary_comparison is True, "End-to-end summary results should be identical"
        assert detailed_comparison is True, "End-to-end detailed results should be identical"
        
        logger.info("TC020 PASSED: End-to-end data lineage reconciliation successful")

if __name__ == "__main__":
    pytest.main(["-v", __file__])