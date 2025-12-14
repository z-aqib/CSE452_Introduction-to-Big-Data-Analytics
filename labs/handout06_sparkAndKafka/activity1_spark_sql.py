from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, sum as spark_sum, count as spark_count, row_number, round as spark_round
from pyspark.sql.window import Window
import time

print("=" * 70)
print("Activity 1: Spark SQL Fundamentals")
print("=" * 70)

# Create Spark Session
spark = SparkSession.builder \
    .appName("Activity1-SparkSQL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"Spark Version: {spark.version}")
print(f"Running Mode: {spark.sparkContext.master}")
# Load dataset
print("=" * 70)
print("STEP 1: Loading Dataset")
print("=" * 70)
start_time = time.time()

# Load each month separately and normalize schema
df_jan = spark.read.parquet("data/yellow_tripdata_2023-01.parquet")
df_feb = spark.read.parquet("data/yellow_tripdata_2023-02.parquet")
df_mar = spark.read.parquet("data/yellow_tripdata_2023-03.parquet")

# Make sure they all have the same columns (use Jan as reference)
common_cols = df_jan.columns
df_feb = df_feb.select(common_cols)
df_mar = df_mar.select(common_cols)

# Normalize passenger_count type across all files
from pyspark.sql.functions import col

df_jan = df_jan.withColumn("passenger_count", col("passenger_count").cast("double"))
df_feb = df_feb.withColumn("passenger_count", col("passenger_count").cast("double"))
df_mar = df_mar.withColumn("passenger_count", col("passenger_count").cast("double"))

# Union into a single DataFrame
df = df_jan.unionByName(df_feb).unionByName(df_mar)

load_time = time.time() - start_time
print(f"✓ Schema loaded in {load_time:.2f} seconds")
start_time = time.time()
count = df.count()
count_time = time.time() - start_time
print(f"✓ Total Records: {count:,}")
print(f"✓ Count time: {count_time:.2f} seconds")

print("=" * 70)
print("STEP 2: Data Schema and Sample")
print("=" * 70)
df.printSchema()
df.show(10, truncate=False)
# SQL Queries
print("=" * 70)
print("STEP 3: SQL Query - Average Fare by Passenger Count")
print("=" * 70)
start_time = time.time()
df.createOrReplaceTempView("trips")
result1 = spark.sql("""
 SELECT 
    passenger_count,
    COUNT(*) as trip_count,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(AVG(tip_amount), 2) as avg_tip,
    ROUND(AVG(trip_distance), 2) as avg_distance
 FROM trips
 GROUP BY passenger_count
 ORDER BY passenger_count
""")
result1.show()
query1_time = time.time() - start_time
print(f"✓ Query time: {query1_time:.2f} seconds")

print("=" * 70)
print("STEP 4: SQL Query - Payment Type Analysis")
print("=" * 70)
start_time = time.time()
result2 = spark.sql("""
 SELECT 
    payment_type,
    COUNT(*) as transaction_count,
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(AVG(total_amount), 2) as avg_transaction
 FROM trips
 GROUP BY payment_type
 ORDER BY total_revenue DESC
""")
result2.show()
query2_time = time.time() - start_time
print(f"✓ Query time: {query2_time:.2f} seconds")
# Caching
print("=" * 70)
print("STEP 5: Caching Demonstration")
print("=" * 70)

print("Query WITHOUT cache:")
start_time = time.time()
df.groupBy("payment_type").count().show()
time_no_cache = time.time() - start_time
print(f"✓ Time without cache: {time_no_cache:.2f} seconds")

print("Caching dataframe...")
df.cache()
df.count()

print("Query WITH cache:")
start_time = time.time()
df.groupBy("payment_type").count().show()
time_with_cache = time.time() - start_time
print(f"✓ Time with cache: {time_with_cache:.2f} seconds")
print(f"✓ Speedup: {time_no_cache/time_with_cache:.2f}x")

spark.stop()
print("✓ Activity 1 Complete!")
