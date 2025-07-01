# PySpark script converted from Talend Java code

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, min, max, collect_list

# Initialize Spark session
spark = SparkSession.builder.appName("Data Transformation Pipeline").getOrCreate()

# Load data into DataFrame
employee_df = spark.read.format("jdbc").options(
    url="jdbc:postgresql://<Host>:<Port>/<Database>",
    dbtable="employee",
    user="<Useranme>",
    password="<Password>"
).load()

# Aggregation logic
agg_df = employee_df.groupBy("manager_id").agg(
    count("id").alias("count"),
    collect_list("name").alias("name_list")
)

# Normalize data
normalized_df = agg_df.select(
    col("manager_id"),
    col("name_list")
)

# Join with salary data
salary_df = spark.read.format("jdbc").options(
    url="jdbc:postgresql://<Host>:<Port>/<Database>",
    dbtable="employee",
    user="<Useranme>",
    password="<Password>"
).load()

final_df = normalized_df.join(salary_df, normalized_df.manager_id == salary_df.id, "inner")

# Write output to file
final_df.write.format("csv").option("header", "true").save("<Filepath>")

# Stop Spark session
spark.stop()

# Note: Replace <Host>, <Port>, <Database>, <Useranme>, <Password>, and <Filepath> with actual values.

# Cost consumed by API for this call: 0.05 units