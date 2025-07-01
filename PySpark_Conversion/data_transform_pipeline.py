import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, collect_list, explode, split

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

try:
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Employee Data Processing") \
        .getOrCreate()

    # Database connection details
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "my_database")
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

    # Load employee data
    employee_df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "employee_table") \
        .option("user", "username") \
        .option("password", "password") \
        .load()

    # Validate required columns
    required_columns = {"Manager_id", "Name", "Id", "Salary"}
    if not required_columns.issubset(employee_df.columns):
        raise ValueError(f"Missing required columns: {required_columns - set(employee_df.columns)}")

    # Aggregation: Group by Manager_id and concatenate Name values
    aggregated_df = employee_df.groupBy("Manager_id") \
        .agg(concat_ws(",", collect_list("Name")).alias("Name_list"))

    # Normalize: Split Name_list into individual rows
    normalized_df = aggregated_df.select(
        col("Manager_id"),
        explode(split(col("Name_list"), ",")).alias("Name")
    )

    # Select salary data
    salary_df = employee_df.select("Id", "Salary")

    # Join employee data with salary data
    joined_df = normalized_df.join(salary_df, normalized_df.Manager_id == salary_df.Id, "inner") \
        .select(normalized_df.Manager_id, normalized_df.Name, salary_df.Salary)

    # Write the final output to a CSV file
    output_path = sys.argv[1] if len(sys.argv) > 1 else "output_file_path.csv"
    joined_df.write.format("csv") \
        .option("header", "true") \
        .option("delimiter", ";") \
        .save(output_path)

except Exception as e:
    logger.error(f"Error occurred: {e}")

finally:
    # Stop Spark session
    spark.stop()