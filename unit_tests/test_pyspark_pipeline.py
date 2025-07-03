import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local").appName("Pytest for PySpark").getOrCreate()

def test_employee_data_transformation(spark_session):
    # Sample input data
    input_data = [("1", "Alice", "", "2"),
                  ("2", "Bob", "", "3"),
                  ("3", "Charlie", "", "")]
    input_schema = ["Id", "Name", "Employee", "Manager_id"]

    # Create DataFrame
    input_df = spark_session.createDataFrame(input_data, input_schema)

    # Transformation logic (as per the PySpark script)
    aggregated_df = input_df.groupBy("Manager_id").agg(collect_list("Name").alias("Name_list"))
    exploded_df = aggregated_df.withColumn("Name", explode(col("Name_list")))

    # Expected output data
    expected_data = [("2", "Alice"),
                     ("3", "Bob"),
                     ("", "Charlie")]
    expected_schema = ["Manager_id", "Name"]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    # Assert transformations
    assert exploded_df.select("Manager_id", "Name").collect() == expected_df.collect()