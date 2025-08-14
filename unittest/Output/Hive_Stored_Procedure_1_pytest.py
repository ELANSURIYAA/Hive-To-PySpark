import pytest
import logging
from datetime import datetime
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql.utils import AnalysisException

class TestHiveStoredProcedurePySpark:
    """
    Comprehensive test suite for Hive to PySpark converted stored procedure.
    Tests cover process_sales_data, validate_date_format, and get_sales_statistics functions.
    """
    
    @classmethod
    def setup_class(cls):
        """Set up SparkSession for all tests"""
        cls.spark = SparkSession.builder \n            .appName("TestProcessSalesData") \n            .master("local[2]") \n            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \n            .config("spark.sql.adaptive.enabled", "false") \n            .getOrCreate()
        
        cls.spark.sparkContext.setLogLevel("WARN")
        
        # Define schema for test data
        cls.sales_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", DateType(), True)
        ])
    
    @classmethod
    def teardown_class(cls):
        """Clean up SparkSession after all tests"""
        cls.spark.stop()
    
    def setup_method(self):
        """Set up before each test method"""
        try:
            self.spark.sql("DROP TABLE IF EXISTS sales_table")
            self.spark.sql("DROP TABLE IF EXISTS summary_table")
            self.spark.sql("DROP TABLE IF EXISTS detailed_sales_summary")
            self.spark.catalog.dropTempView("temp_sales_summary")
        except:
            pass
    
    def teardown_method(self):
        """Clean up after each test method"""
        try:
            self.spark.sql("DROP TABLE IF EXISTS sales_table")
            self.spark.sql("DROP TABLE IF EXISTS summary_table")
            self.spark.sql("DROP TABLE IF EXISTS detailed_sales_summary")
            self.spark.catalog.dropTempView("temp_sales_summary")
        except:
            pass
    
    def create_mock_sales_data(self, data):
        """Helper method to create mock sales data"""
        df = self.spark.createDataFrame(data, self.sales_schema)
        df.createOrReplaceTempView("sales_table")
        return df
    
    def validate_date_format(self, date_string):
        """Utility function to test - validates date format"""
        try:
            datetime.strptime(date_string, '%Y-%m-%d')
            return True
        except (ValueError, TypeError):
            return False
    
    def get_sales_statistics(self, start_date, end_date):
        """Utility function to test - gets sales statistics"""
        sales_df = self.spark.table("sales_table")
        
        filtered_df = sales_df.filter(
            (col("sale_date") >= start_date) & 
            (col("sale_date") <= end_date)
        )
        
        total_records = filtered_df.count()
        unique_products = filtered_df.select("product_id").distinct().count()
        total_sales_result = filtered_df.agg(spark_sum("sales")).collect()[0][0]
        total_sales_amount = total_sales_result if total_sales_result is not None else 0
        
        stats = {
            'total_records': total_records,
            'unique_products': unique_products,
            'total_sales_amount': total_sales_amount
        }
        
        return stats
    
    def process_sales_data(self, start_date, end_date):
        """Main function to test - processes sales data"""
        logger = logging.getLogger(__name__)
        
        try:
            logger.info(f"Starting sales data processing for period: {start_date} to {end_date}")
            
            sales_df = self.spark.table("sales_table")
            
            if sales_df.count() == 0:
                logger.warning("No data found in sales_table")
                return
            
            filtered_sales_df = sales_df.filter(
                (col("sale_date") >= start_date) & 
                (col("sale_date") <= end_date)
            )
            
            logger.info(f"Filtered sales data count: {filtered_sales_df.count()}")
            
            summary_df = filtered_sales_df.groupBy("product_id") \n                .agg(spark_sum("sales").alias("total_sales")) \n                .orderBy("product_id")
            
            summary_df.cache()
            
            logger.info(f"Summary data count: {summary_df.count()}")
            
            summary_df.write \n                .mode("overwrite") \n                .option("mergeSchema", "true") \n                .saveAsTable("summary_table")
            
            logger.info("Data successfully inserted into summary_table")
            
            summary_df.createOrReplaceTempView("temp_sales_summary")
            
            summary_rows = summary_df.collect()
            detailed_records = []
            
            for row in summary_rows:
                product_id = row['product_id']
                total_sales = row['total_sales']
                
                if total_sales is not None:
                    detailed_records.append((product_id, total_sales))
                    logger.debug(f"Processed product_id: {product_id}, total_sales: {total_sales}")
            
            if detailed_records:
                detailed_schema = StructType([
                    StructField("product_id", StringType(), True),
                    StructField("total_sales", FloatType(), True)
                ])
                
                detailed_df = self.spark.createDataFrame(detailed_records, detailed_schema)
                
                detailed_df.write \n                    .mode("overwrite") \n                    .option("mergeSchema", "true") \n                    .saveAsTable("detailed_sales_summary")
                
                logger.info(f"Inserted {len(detailed_records)} records into detailed_sales_summary")
            
            summary_df.unpersist()
            self.spark.catalog.dropTempView("temp_sales_summary")
            
            logger.info("Sales data processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error in process_sales_data: {str(e)}")
            raise e
    
    # TC001: Test process_sales_data with valid date range and normal data
    def test_process_sales_data_valid_date_range_normal_data(self):
        """TC001: Test process_sales_data with valid date range and normal data"""
        test_data = [
            ("P001", 100.0, datetime.strptime("2023-01-15", "%Y-%m-%d").date()),
            ("P002", 200.0, datetime.strptime("2023-01-20", "%Y-%m-%d").date()),
            ("P001", 150.0, datetime.strptime("2023-01-25", "%Y-%m-%d").date()),
            ("P003", 300.0, datetime.strptime("2023-02-10", "%Y-%m-%d").date())
        ]
        self.create_mock_sales_data(test_data)
        
        self.process_sales_data("2023-01-01", "2023-01-31")
        
        summary_df = self.spark.table("summary_table")
        assert summary_df.count() == 2  # P001 and P002
        
        p001_total = summary_df.filter(col("product_id") == "P001").collect()[0]["total_sales"]
        assert p001_total == 250.0  # 100 + 150
        
        detailed_df = self.spark.table("detailed_sales_summary")
        assert detailed_df.count() == 2
    
    # TC002: Test process_sales_data with empty sales table
    def test_process_sales_data_empty_sales_table(self):
        """TC002: Test process_sales_data with empty sales table"""
        self.create_mock_sales_data([])
        
        with patch('logging.getLogger') as mock_logger:
            mock_log_instance = MagicMock()
            mock_logger.return_value = mock_log_instance
            
            self.process_sales_data("2023-01-01", "2023-01-31")
            
            mock_log_instance.warning.assert_called_with("No data found in sales_table")
    
    # TC003: Test process_sales_data with date range that returns no records
    def test_process_sales_data_no_records_in_date_range(self):
        """TC003: Test process_sales_data with date range that returns no records"""
        test_data = [
            ("P001", 100.0, datetime.strptime("2022-01-15", "%Y-%m-%d").date()),
            ("P002", 200.0, datetime.strptime("2022-01-20", "%Y-%m-%d").date())
        ]
        self.create_mock_sales_data(test_data)
        
        self.process_sales_data("2023-01-01", "2023-01-31")
        
        summary_df = self.spark.table("summary_table")
        assert summary_df.count() == 0
    
    # TC004: Test process_sales_data with null values in sales data
    def test_process_sales_data_with_null_values(self):
        """TC004: Test process_sales_data with null values in sales data"""
        test_data = [
            ("P001", 100.0, datetime.strptime("2023-01-15", "%Y-%m-%d").date()),
            ("P002", None, datetime.strptime("2023-01-20", "%Y-%m-%d").date()),
            ("P001", 150.0, datetime.strptime("2023-01-25", "%Y-%m-%d").date())
        ]
        self.create_mock_sales_data(test_data)
        
        self.process_sales_data("2023-01-01", "2023-01-31")
        
        summary_df = self.spark.table("summary_table")
        assert summary_df.count() == 2  # P001 and P002
        
        p001_total = summary_df.filter(col("product_id") == "P001").collect()[0]["total_sales"]
        assert p001_total == 250.0
    
    # TC005: Test process_sales_data with invalid date format parameters
    def test_process_sales_data_invalid_date_format(self):
        """TC005: Test process_sales_data with invalid date format parameters"""
        test_data = [
            ("P001", 100.0, datetime.strptime("2023-01-15", "%Y-%m-%d").date())
        ]
        self.create_mock_sales_data(test_data)
        
        with pytest.raises(Exception):
            self.process_sales_data("invalid-date", "2023-01-31")
    
    # TC006: Test process_sales_data with start_date > end_date
    def test_process_sales_data_inverted_date_range(self):
        """TC006: Test process_sales_data with start_date > end_date"""
        test_data = [
            ("P001", 100.0, datetime.strptime("2023-01-15", "%Y-%m-%d").date())
        ]
        self.create_mock_sales_data(test_data)
        
        self.process_sales_data("2023-01-31", "2023-01-01")
        
        summary_df = self.spark.table("summary_table")
        assert summary_df.count() == 0
    
    # TC007: Test process_sales_data with single day date range
    def test_process_sales_data_single_day_range(self):
        """TC007: Test process_sales_data with single day date range"""
        test_data = [
            ("P001", 100.0, datetime.strptime("2023-01-15", "%Y-%m-%d").date()),
            ("P002", 200.0, datetime.strptime("2023-01-15", "%Y-%m-%d").date())
        ]
        self.create_mock_sales_data(test_data)
        
        self.process_sales_data("2023-01-15", "2023-01-15")
        
        summary_df = self.spark.table("summary_table")
        assert summary_df.count() == 2
    
    # TC008: Test process_sales_data with large dataset simulation
    def test_process_sales_data_large_dataset(self):
        """TC008: Test process_sales_data with large dataset simulation"""
        test_data = [(f"P{i:03d}", float(i * 10), datetime.strptime("2023-01-15", "%Y-%m-%d").date())