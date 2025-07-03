import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local").appName("pytest_spark_testing").getOrCreate()

@pytest.fixture(scope="module")
def input_data(spark_session):
    data = [
        (1, "John", "Engineer", 101, 60000),
        (2, "Jane", "Manager", 102, 45000),
        (3, "Doe", "Analyst", 103, 70000),
        (4, "Smith", "Clerk", 104, 30000)
    ]
    schema = ["id", "name", "employee", "manager_id", "salary"]
    return spark_session.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def expected_output_data(spark_session):
    data = [
        (1, "John", "Engineer", 101, 60000),
        (3, "Doe", "Analyst", 103, 70000)
    ]
    schema = ["id", "name", "employee", "manager_id", "salary"]
    return spark_session.createDataFrame(data, schema)

@pytest.mark.usefixtures("spark_session")
def test_filter_transformation(spark_session, input_data, expected_output_data):
    filtered_df = input_data.filter(col("salary") > 50000).select("id", "name", "employee", "manager_id", "salary")

    assert filtered_df.count() == expected_output_data.count()
    assert filtered_df.collect() == expected_output_data.collect()

@pytest.mark.usefixtures("spark_session")
def test_schema_validation(spark_session, input_data):
    expected_schema = ["id", "name", "employee", "manager_id", "salary"]
    assert input_data.schema.names == expected_schema

@pytest.mark.usefixtures("spark_session")
def test_empty_dataset(spark_session):
    empty_df = spark_session.createDataFrame([], ["id", "name", "employee", "manager_id", "salary"])
    filtered_df = empty_df.filter(col("salary") > 50000)

    assert filtered_df.count() == 0

@pytest.mark.usefixtures("spark_session")
def test_null_values(spark_session):
    data_with_nulls = [
        (None, "John", "Engineer", 101, 60000),
        (2, None, "Manager", 102, 45000),
        (3, "Doe", None, 103, 70000),
        (4, "Smith", "Clerk", None, None)
    ]
    schema = ["id", "name", "employee", "manager_id", "salary"]
    df_with_nulls = spark_session.createDataFrame(data_with_nulls, schema)

    filtered_df = df_with_nulls.filter(col("salary") > 50000)

    assert filtered_df.count() == 2