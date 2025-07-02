# Pytest script for testing PySpark pipeline

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local[1]").appName("TestPipeline").getOrCreate()

@pytest.fixture(scope="module")
def employee_data():
    return [
        {"id": 1, "name": "Alice", "manager_id": 101, "salary": 5000},
        {"id": 2, "name": "Bob", "manager_id": 101, "salary": 6000},
        {"id": 3, "name": "Charlie", "manager_id": 102, "salary": 7000},
    ]

@pytest.fixture(scope="module")
def employee_df(spark_session, employee_data):
    return spark_session.createDataFrame(employee_data)

@pytest.fixture(scope="module")
def salary_data():
    return [
        {"id": 101, "salary": 8000},
        {"id": 102, "salary": 9000},
    ]

@pytest.fixture(scope="module")
def salary_df(spark_session, salary_data):
    return spark_session.createDataFrame(salary_data)

def test_aggregation(employee_df):
    agg_df = employee_df.groupBy("manager_id").agg(F.collect_list("name").alias("name_list"))
    assert agg_df.count() == 2
    assert set(agg_df.columns) == {"manager_id", "name_list"}

def test_explode_and_drop(employee_df):
    agg_df = employee_df.groupBy("manager_id").agg(F.collect_list("name").alias("name_list"))
    exploded_df = agg_df.withColumn("name", F.explode(F.col("name_list"))).drop("name_list")
    assert exploded_df.count() == 3
    assert set(exploded_df.columns) == {"manager_id", "name"}

def test_join(employee_df, salary_df):
    agg_df = employee_df.groupBy("manager_id").agg(F.collect_list("name").alias("name_list"))
    exploded_df = agg_df.withColumn("name", F.explode(F.col("name_list"))).drop("name_list")
    final_df = exploded_df.join(salary_df, exploded_df.manager_id == salary_df.id, "inner").select(exploded_df.manager_id, exploded_df.name, salary_df.salary)
    assert final_df.count() == 3
    assert set(final_df.columns) == {"manager_id", "name", "salary"}

def test_write_csv(spark_session, employee_df, salary_df, tmp_path):
    agg_df = employee_df.groupBy("manager_id").agg(F.collect_list("name").alias("name_list"))
    exploded_df = agg_df.withColumn("name", F.explode(F.col("name_list"))).drop("name_list")
    final_df = exploded_df.join(salary_df, exploded_df.manager_id == salary_df.id, "inner").select(exploded_df.manager_id, exploded_df.name, salary_df.salary)
    output_path = tmp_path / "output.csv"
    final_df.write.csv(str(output_path), header=True)
    assert output_path.exists()

# Test case list
# 1. Test aggregation logic: Ensure grouping and aggregation are correct.
# 2. Test explode and drop: Validate transformation and schema.
# 3. Test join: Verify correctness of join operation.
# 4. Test write CSV: Confirm output file is generated correctly.
