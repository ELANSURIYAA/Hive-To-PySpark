import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, lit, split

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local").appName("unit-tests").getOrCreate()

@pytest.fixture(scope="module")
def employee_data(spark_session):
    data = [(1, "Alice", 101), (2, "Bob", 101), (3, "Charlie", 102)]
    schema = ["id", "name", "manager_id"]
    return spark_session.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def salary_data(spark_session):
    data = [(101, 5000), (102, 6000)]
    schema = ["id", "salary"]
    return spark_session.createDataFrame(data, schema)

def test_aggregation(employee_data):
    aggregated_df = employee_data.groupBy("manager_id").agg(collect_list("name").alias("name_list"))
    result = aggregated_df.collect()
    assert len(result) == 2
    assert result[0]["manager_id"] == 101
    assert "Alice" in result[0]["name_list"]
    assert "Bob" in result[0]["name_list"]

def test_normalization(employee_data):
    aggregated_df = employee_data.groupBy("manager_id").agg(collect_list("name").alias("name_list"))
    normalized_df = aggregated_df.withColumn("name", split(col("name_list"), ",")).select("manager_id", "name")
    assert "name" in normalized_df.columns

def test_join(employee_data, salary_data):
    aggregated_df = employee_data.groupBy("manager_id").agg(collect_list("name").alias("name_list"))
    normalized_df = aggregated_df.withColumn("name", split(col("name_list"), ",")).select("manager_id", "name")
    joined_df = normalized_df.join(salary_data, normalized_df.manager_id == salary_data.id, "inner").select(normalized_df.manager_id, normalized_df.name, salary_data.salary)
    assert "salary" in joined_df.columns
    assert joined_df.count() == 2

def test_empty_dataset(spark_session):
    empty_df = spark_session.createDataFrame([], ["id", "name", "manager_id"])
    aggregated_df = empty_df.groupBy("manager_id").agg(collect_list("name").alias("name_list"))
    assert aggregated_df.count() == 0

def test_schema_mismatch(spark_session):
    data = [(1, "Alice")]
    schema = ["id", "name"]
    invalid_df = spark_session.createDataFrame(data, schema)
    with pytest.raises(Exception):
        invalid_df.groupBy("manager_id").agg(collect_list("name").alias("name_list"))
