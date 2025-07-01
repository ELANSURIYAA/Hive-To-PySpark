from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, min, max, collect_list

# Initialize Spark Session
spark = SparkSession.builder.appName("Talend to PySpark Conversion").getOrCreate()

# Load data into DataFrame
employee_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://<Host>:<Port>/<Database>") \n    .option("dbtable", "employee") \n    .option("user", "<Useranme>") \n    .option("password", "<Password>") \n    .load()

# Perform transformations
# tAggregateRow_1 mapped to agg
agg_df = employee_df.groupBy("Manager_id").agg(
    collect_list("Name").alias("Name_list")
)

# Normalize Name_list
normalized_df = agg_df.withColumn("Name", col("Name_list").cast("string"))

# Join with salary data
salary_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://<Host>:<Port>/<Database>") \n    .option("dbtable", "employee") \n    .option("user", "<Useranme>") \n    .option("password", "<Password>") \n    .load()

final_df = normalized_df.join(salary_df, normalized_df.Manager_id == salary_df.Id, "inner") \n    .select(normalized_df.Manager_id, normalized_df.Name, salary_df.Salary)

# Write final DataFrame to file
final_df.write.csv("<Filepath>", header=True)

# Stop Spark Session
spark.stop()