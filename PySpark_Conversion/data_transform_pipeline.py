from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

# Initialize Spark Session
spark = SparkSession.builder.appName("Data Transformation Pipeline").getOrCreate()

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

# Aggregation: Group by Manager_id and concatenate names
aggregated_df = employee_df.groupBy("Manager_id") \
    .agg(concat_ws(",", col("Name")).alias("Name_list"))

# Normalize: Split Name_list into individual names
normalized_df = aggregated_df.selectExpr("Manager_id", "explode(split(Name_list, ',')) as Name")

# Join with salary data
salary_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "employee") \
    .option("user", username) \
    .option("password", password) \
    .load()

joined_df = normalized_df.join(salary_df.select("Id", "Salary"), normalized_df["Manager_id"] == salary_df["Id"], "inner")

# Write the final output to a CSV file
output_path = "<Filepath>"
joined_df.write.format("csv") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .save(output_path)

# Stop Spark Session
spark.stop()