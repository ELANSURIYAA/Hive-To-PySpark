import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("Data Transformation Pipeline").getOrCreate()

# Database connection details as environment variables
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

# JDBC URL
jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

# Load data from the employee table
employee_df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "employee").option("user", username).option("password", password).load()

# Perform aggregation (group by manager_id and collect names)
agg_df = employee_df.groupBy("manager_id").agg(F.collect_list("name").alias("name_list"))

# Explode name_list into individual rows
exploded_df = agg_df.withColumn("name", F.explode(F.col("name_list"))).drop("name_list")

# Load salary data from the employee table
salary_df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "employee").option("user", username).option("password", password).select("id", "salary")

# Join exploded data with salary data
final_df = exploded_df.join(salary_df, exploded_df.manager_id == salary_df.id, "inner").select(exploded_df.manager_id, exploded_df.name, salary_df.salary)

# Write the final output to a CSV file
output_path = os.getenv("OUTPUT_FILE_PATH")
final_df.write.csv(output_path, header=True)

# Stop Spark Session
spark.stop()