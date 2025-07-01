from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, collect_list, explode, split

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Transformation Pipeline") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

# Database connection details
import os


# JDBC URL
jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

try:
    # Read data from the employee table
    employee_df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "employee") \
        .option("user", username) \
        .option("password", password) \
        .load()

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
    import sys
    output_path = sys.argv[1] if len(sys.argv) > 1 else "output_file_path.csv"
    joined_df.write.format("csv") \
        .option("header", "true") \
        .option("delimiter", ";") \
        .save(output_path)

except Exception as e:
    print(f"Error occurred: {e}")

finally:
    # Stop Spark session
    spark.stop()