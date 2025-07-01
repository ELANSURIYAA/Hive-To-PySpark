from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Data Transformation Pipeline") \
    .getOrCreate()

# Database connection details
host = "<Host>"
port = "<Port>"
database = "<Database>"
username = "<Username>"
password = "<Password>"

# JDBC URL
jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

# Read data from the database
employee_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "employee") \
    .option("user", username) \
    .option("password", password) \
    .load()

# Perform aggregation
aggregated_df = employee_df.groupBy("manager_id") \
    .agg(concat_ws(",", col("name")).alias("name_list"))

# Normalize the data
normalized_df = aggregated_df.withColumn("name", col("name_list")) \
    .drop("name_list")

# Read salary data
salary_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "employee") \
    .option("user", username) \
    .option("password", password) \
    .load()

# Perform join operation
final_df = normalized_df.join(salary_df, normalized_df.manager_id == salary_df.id, "inner") \
    .select(normalized_df.manager_id.alias("Id"),
            normalized_df.name.alias("Name"),
            col("employee").alias("Employee"),
            col("manager_id").alias("Manager_id"),
            col("salary").alias("Salary"))

# Write the final output to a CSV file
output_path = "<Filepath>"
final_df.write.format("csv") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .save(output_path)

# Stop Spark Session
spark.stop()