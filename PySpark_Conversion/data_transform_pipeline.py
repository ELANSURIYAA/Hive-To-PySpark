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

# JDBC URL
jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

# Load data from the database
employee_df = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="org.postgresql.Driver",
    dbtable="employee",
    user=username,
    password=password
).load()

# Perform aggregation
aggregated_df = employee_df.groupBy("manager_id").agg(
    collect_list("name").alias("name_list")
)

# Normalize the aggregated data
normalized_df = aggregated_df.select(
    col("manager_id"),
    concat_ws(",", col("name_list")).alias("normalized_names")
)

# Load salary data
salary_df = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="org.postgresql.Driver",
    dbtable="employee",
    user=username,
    password=password
).load().select("id", "salary")

# Join normalized data with salary data
final_df = normalized_df.join(salary_df, normalized_df.manager_id == salary_df.id, "inner")

# Write the final output to a CSV file
output_path = "<Filepath>"
final_df.write.format("csv").option("header", "true").save(output_path)

# Stop Spark Session
spark.stop()