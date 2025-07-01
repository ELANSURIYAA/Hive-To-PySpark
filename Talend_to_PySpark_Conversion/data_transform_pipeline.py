from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Talend to PySpark Conversion") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

try:
    # Load data into DataFrame
    employee_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://<Host>:<Port>/<Database>") \
        .option("dbtable", "employee") \
        .option("user", "<Username>") \
        .option("password", "<Password>") \
        .load()

    # Perform transformations
    agg_df = employee_df.groupBy("Manager_id").agg(
        collect_list("Name").alias("Name_list")
    )

    # Normalize Name_list
    normalized_df = agg_df.withColumn("Name", col("Name_list").cast("string"))  # Consider joining list elements into a string

    # Load salary data (assuming salary is a separate table)
    salary_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://<Host>:<Port>/<Database>") \
        .option("dbtable", "salary") \
        .option("user", "<Username>") \
        .option("password", "<Password>") \
        .load()

    # Join DataFrames
    final_df = normalized_df.join(salary_df, normalized_df.Manager_id == salary_df.Id, "inner") \
        .select(normalized_df.Manager_id, normalized_df.Name, salary_df.Salary)

    # Write final DataFrame to file
    final_df.write.csv("<Filepath>", header=True, mode="overwrite")

except Exception as e:
    print(f"Error occurred: {e}")

finally:
    # Stop Spark Session
    spark.stop()
