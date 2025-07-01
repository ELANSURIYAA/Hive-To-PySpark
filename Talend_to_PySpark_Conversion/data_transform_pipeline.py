from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, collect_list

# Initialize Spark Session
spark = SparkSession.builder.appName("Data Transformation Pipeline").getOrCreate()

# Database connection properties
host = "<Host>"
port = "<Port>"
database = "<Database>"
username = "<Username>"
password = "<Password>"

url = f"jdbc:postgresql://{host}:{port}/{database}"
properties = {
    "user": username,
    "password": password,
    "driver": "org.postgresql.Driver"
}

# Load data from PostgreSQL
employee_df = spark.read.jdbc(url=url, table="employee", properties=properties)

# Perform aggregation
aggregated_df = employee_df.groupBy("manager_id").agg(
    collect_list("name").alias("name_list")
)

# Normalize the aggregated data
normalized_df = aggregated_df.withColumn("name", concat_ws(",", col("name_list"))).drop("name_list")

# Load salary data
salary_df = spark.read.jdbc(url=url, table="employee", properties=properties).select("id", "salary")

# Join normalized data with salary data
final_df = normalized_df.join(salary_df, normalized_df.manager_id == salary_df.id, "inner")

# Write the final data to a CSV file
output_path = "<Filepath>"
final_df.write.option("header", "true").csv(output_path)

# Stop Spark Session
spark.stop()

# Note: Replace placeholders <Host>, <Port>, <Database>, <Username>, <Password>, and <Filepath> with actual values.