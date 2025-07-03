from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, collect_list

# Initialize Spark Session
spark = SparkSession.builder.appName("ETL_Filter_And_Export").getOrCreate()

# Load data from PostgreSQL database
jdbc_url = "jdbc:postgresql://<Host>:<Port>/<Database>"
properties = {
    "user": "<Username>",
    "password": "<Password>"
}

# Read employee table
employee_df = spark.read.jdbc(url=jdbc_url, table="employee", properties=properties)

# Aggregation logic: Group by Manager_id and aggregate Name list
agg_df = employee_df.groupBy("Manager_id").agg(
    collect_list("Name").alias("Name_list")
)

# Explode Name_list into individual rows
exploded_df = agg_df.select(
    col("Manager_id"),
    col("Name_list")
).withColumn("Name", explode(col("Name_list")))

# Write the output to a CSV file
output_path = "<Filepath>"
exploded_df.write.csv(output_path, header=True)

# Stop Spark Session
spark.stop()