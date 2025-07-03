from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit

# Initialize Spark Session
spark = SparkSession.builder.appName("ETL_Filter_and_Export").getOrCreate()

# Database connection details
host = "<Host>"
port = "<Port>"
database = "<Database>"
username = "<Useranme>"
password = "<Password>"

# Load data from PostgreSQL
url = f"jdbc:postgresql://{host}:{port}/{database}"
properties = {
    "user": username,
    "password": password,
    "driver": "org.postgresql.Driver"
}

employee_df = spark.read.jdbc(url=url, table="employee", properties=properties)

# Transformation: Aggregation
agg_df = employee_df.groupBy("manager_id").agg(
    concat_ws(",", col("name")).alias("name_list")
)

# Transformation: Normalize
normalized_df = agg_df.withColumn("name", concat_ws(",", col("name_list")))

# Load Salary Data
salary_df = spark.read.jdbc(url=url, table="employee", properties=properties).select("id", "salary")

# Join with Salary Data
final_df = normalized_df.join(salary_df, normalized_df.manager_id == salary_df.id, "inner")

# Export to CSV
output_path = "<Filepath>"
final_df.write.option("header", "true").csv(output_path)

# Stop Spark Session
spark.stop()