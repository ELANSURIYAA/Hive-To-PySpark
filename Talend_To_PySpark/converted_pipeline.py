from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count

# Initialize Spark session
spark = SparkSession.builder.appName("Talend_to_PySpark_Conversion").getOrCreate()

# Database connection details
host = "<Host>"
port = "<Port>"
database = "<Database>"
username = "<Useranme>"
password = "<Password>"

# JDBC URL
jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

# Read data from PostgreSQL table
employee_df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "employee").option("user", username).option("password", password).load()

# Perform aggregation
agg_df = employee_df.groupBy("manager_id").agg(
    collect_list("name").alias("name_list"),
    count("id").alias("count")
)

# Normalize the data
normalized_df = agg_df.withColumn("name", col("name_list").cast("string"))

# Write the output to a file
output_path = "<Filepath>"
normalized_df.write.csv(output_path, header=True, sep=";")

# Stop Spark session
spark.stop()