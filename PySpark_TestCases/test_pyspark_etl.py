import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, explode

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local").appName("pytest-pyspark").getOrCreate()

@pytest.fixture(scope="module")
def employee_data():
    return [
        {"Id": "1", "Name": "Alice", "Manager_id": "101"},
        {"Id": "2", "Name": "Bob", "Manager_id": "101"},
        {"Id": "3", "Name": "Charlie", "Manager_id": "102"}
    ]

@pytest.fixture(scope="module")
def salary_data():
    return [
        {"Id": "101", "Salary": "1000"},
        {"Id": "102", "Salary": "2000"}
    ]

@pytest.fixture(scope="module")
def employee_df(spark_session, employee_data):
    return spark_session.createDataFrame(employee_data)

@pytest.fixture(scope="module")
def salary_df(spark_session, salary_data):
    return spark_session.createDataFrame(salary_data)

@pytest.fixture(scope="module")
def aggregated_df(employee_df):
    return employee_df.groupBy("Manager_id").agg(collect_list("Name").alias("Name_list"))

@pytest.fixture(scope="module")
def normalized_df(aggregated_df):
    return aggregated_df.withColumn("Name", explode(col("Name_list"))).drop("Name_list")

@pytest.fixture(scope="module")
def final_df(normalized_df, salary_df):
    return normalized_df.join(salary_df, normalized_df.Manager_id == salary_df.Id, "inner").select(
        normalized_df.Manager_id,
        normalized_df.Name,
        salary_df.Salary
    )

def test_aggregation(employee_df, aggregated_df):
    assert aggregated_df.count() == 2
    assert aggregated_df.filter(col("Manager_id") == "101").select("Name_list").collect()[0][0] == ["Alice", "Bob"]

def test_normalization(aggregated_df, normalized_df):
    assert normalized_df.count() == 3
    assert "Name_list" not in normalized_df.columns

def test_join(normalized_df, salary_df, final_df):
    assert final_df.count() == 3
    assert "Salary" in final_df.columns
    assert final_df.filter(col("Name") == "Alice").select("Salary").collect()[0][0] == "1000"

def test_schema(final_df):
    expected_schema = ["Manager_id", "Name", "Salary"]
    assert final_df.columns == expected_schema

def test_empty_dataset(spark_session):
    empty_df = spark_session.createDataFrame([], schema="Manager_id STRING, Name STRING, Salary STRING")
    assert empty_df.count() == 0

def test_null_values(spark_session):
    data_with_nulls = [
        {"Manager_id": None, "Name": "Alice", "Salary": "1000"},
        {"Manager_id": "102", "Name": None, "Salary": "2000"}
    ]
    df_with_nulls = spark_session.createDataFrame(data_with_nulls)
    assert df_with_nulls.filter(col("Manager_id").isNull()).count() == 1
    assert df_with_nulls.filter(col("Name").isNull()).count() == 1