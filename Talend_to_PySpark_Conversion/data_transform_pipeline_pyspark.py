from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, collect_list, lit

# Initialize Spark session
spark = SparkSession.builder.appName("Data Transformation Pipeline").getOrCreate()

# Database connection details
host = "your_host"
port = "your_port"
database = "your_database"
username = "your_username"
password = "your_password"

# JDBC URL
jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

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
    col("Name_list")
).withColumn("Name", lit(None))

# Read salary data from the employee table
salary_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "employee") \
    .option("user", username) \
    .option("password", password) \
    .load()

# Join employee data with salary data
joined_df = normalized_df.join(salary_df, normalized_df.Manager_id == salary_df.Id, "inner") \
    .select(normalized_df.Manager_id, normalized_df.Name, salary_df.Salary)

# Write the final output to a CSV file
output_path = "output_file_path.csv"
joined_df.write.format("csv") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .save(output_path)

# Stop Spark session
spark.stop()