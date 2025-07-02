from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, lit, split
import os

# Initialize Spark session
spark = SparkSession.builder.appName("Data Transformation Pipeline").getOrCreate()

# Database connection details from environment variables
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

# JDBC URL
jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

# Read data from the first table
employee_df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "employee").option("user", db_user).option("password", db_password).load()

# Aggregate operation: Group by 'manager_id' and collect 'name' as a list
aggregated_df = employee_df.groupBy("manager_id").agg(collect_list("name").alias("name_list"))

# Normalize the collected list by splitting into individual rows
normalized_df = aggregated_df.withColumn("name", split(col("name_list"), ",")).select("manager_id", "name")

# Read data from the second table
salary_df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "employee").option("user", db_user).option("password", db_password).load()

# Join the normalized data with salary data
joined_df = normalized_df.join(salary_df, normalized_df.manager_id == salary_df.id, "inner").select(normalized_df.manager_id, normalized_df.name, salary_df.salary)

# Write the final output to a CSV file
output_path = os.getenv('OUTPUT_PATH', '/tmp/output.csv')
joined_df.write.csv(output_path, header=True)

# Stop the Spark session
spark.stop()