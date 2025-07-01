import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list

@pytest.fixture(scope="module")
def spark_session():
    """Fixture for creating a SparkSession."""
    spark = SparkSession.builder.master("local").appName("pytest-pyspark-tests").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def sample_employee_data():
    """Fixture for sample employee data."""
    return [
        {"Manager_id": 1, "Name": "Alice"},
        {"Manager_id": 1, "Name": "Bob"},
        {"Manager_id": 2, "Name": "Charlie"},
        {"Manager_id": 2, "Name": "David"},
    ]

@pytest.fixture(scope="module")
def sample_salary_data():
    """Fixture for sample salary data."""
    return [
        {"Manager_id": 1, "Salary": 100000},
        {"Manager_id": 2, "Salary": 120000},
    ]

@pytest.fixture(scope="module")
def employee_df(spark_session, sample_employee_data):
    """Fixture for creating employee DataFrame."""
    return spark_session.createDataFrame(sample_employee_data)

@pytest.fixture(scope="module")
def salary_df(spark_session, sample_salary_data):
    """Fixture for creating salary DataFrame."""
    return spark_session.createDataFrame(sample_salary_data)

# Test Case 1: Validate aggregation logic
def test_aggregation(employee_df):
    agg_df = employee_df.groupBy("Manager_id").agg(
        collect_list("Name").alias("Name_list")
    )
    expected_data = [
        {"Manager_id": 1, "Name_list": ["Alice", "Bob"]},
        {"Manager_id": 2, "Name_list": ["Charlie", "David"]},
    ]
    expected_df = employee_df.sparkSession.createDataFrame(expected_data)
    assert agg_df.collect() == expected_df.collect()

# Test Case 2: Validate normalization logic
def test_normalization(employee_df):
    agg_df = employee_df.groupBy("Manager_id").agg(
        collect_list("Name").alias("Name_list")
    )
    normalized_df = agg_df.withColumn("Name", col("Name_list").cast("array<string>")).select(
        col("Manager_id"), col("Name")
    )
    expected_data = [
        {"Manager_id": 1, "Name": ["Alice", "Bob"]},
        {"Manager_id": 2, "Name": ["Charlie", "David"]},
    ]
    expected_df = employee_df.sparkSession.createDataFrame(expected_data)
    assert normalized_df.collect() == expected_df.collect()

# Test Case 3: Validate join logic
def test_join(employee_df, salary_df):
    agg_df = employee_df.groupBy("Manager_id").agg(
        collect_list("Name").alias("Name_list")
    )
    normalized_df = agg_df.withColumn("Name", col("Name_list").cast("array<string>")).select(
        col("Manager_id"), col("Name")
    )
    final_df = normalized_df.join(salary_df, normalized_df.Manager_id == salary_df.Manager_id, "inner")
    expected_data = [
        {"Manager_id": 1, "Name": ["Alice", "Bob"], "Salary": 100000},
        {"Manager_id": 2, "Name": ["Charlie", "David"], "Salary": 120000},
    ]
    expected_df = employee_df.sparkSession.createDataFrame(expected_data)
    assert final_df.collect() == expected_df.collect()

# Test Case 4: Validate empty dataset handling
def test_empty_dataset(spark_session):
    empty_df = spark_session.createDataFrame([], schema="Manager_id INT, Name STRING")
    agg_df = empty_df.groupBy("Manager_id").agg(
        collect_list("Name").alias("Name_list")
    )
    assert agg_df.count() == 0

# Test Case 5: Validate schema mismatch handling
def test_schema_mismatch(spark_session):
    invalid_data = [{"Manager_id": "invalid", "Name": 123}]
    with pytest.raises(Exception):
        spark_session.createDataFrame(invalid_data)

# Test Case 6: Validate null values handling
def test_null_values(spark_session):
    null_data = [
        {"Manager_id": None, "Name": "Alice"},
        {"Manager_id": 1, "Name": None},
    ]
    df = spark_session.createDataFrame(null_data)
    agg_df = df.groupBy("Manager_id").agg(
        collect_list("Name").alias("Name_list")
    )
    assert agg_df.filter(col("Manager_id").isNull()).count() == 1
    assert agg_df.filter(col("Name_list").isNull()).count() == 0
