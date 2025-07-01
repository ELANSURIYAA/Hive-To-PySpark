from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, collect_list, lit, avg

# Initialize Spark session
spark = SparkSession.builder.appName("Data Transformation Pipeline").getOrCreate()

# Database connection details
host = "<Host>"
port = "<Port>"
database = "<Database>"
username = "<Useranme>"
password = "<Password>"

# JDBC URL
jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

# Load data from PostgreSQL
employee_df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "employee").option("user", username).option("password", password).load()

# Aggregation: Group by Manager_id and collect names
agg_df = employee_df.groupBy("Manager_id").agg(
    collect_list("Name").alias("Name_list")
)

# Normalize: Split names into individual rows
normalized_df = agg_df.withColumn("Name", col("Name_list").cast("array<string>")).select(
    col("Manager_id"), col("Name")
)

# Load salary data
salary_df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "employee").option("user", username).option("password", password).load()

# Join normalized data with salary data
final_df = normalized_df.join(salary_df, normalized_df.Manager_id == salary_df.Manager_id, "inner")

# Write final output to file
output_path = "<Filepath>"
final_df.write.csv(output_path, header=True)

# Stop Spark session
spark.stop()