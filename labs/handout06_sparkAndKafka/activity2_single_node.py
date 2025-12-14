from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, sum as spark_sum, count as spark_count
import time

def run_benchmark(cores, app_name):
    print("\n" + "=" * 70)
    print(f"BENCHMARK: {cores} Core(s)")
    print("=" * 70)

    # Create a SparkSession in *local* mode with 'cores' cores
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{cores}]")  # local[1], local[2], local[4]
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", str(cores * 2))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    results = {}

    # ---------------------------
    # Test 1: Load Data
    # ---------------------------
    print("Test 1: Loading Data...")
    start = time.time()

    # [CHANGED] Load each month separately and normalize schema
    df_jan = spark.read.parquet("data/yellow_tripdata_2023-01.parquet")
    df_feb = spark.read.parquet("data/yellow_tripdata_2023-02.parquet")
    df_mar = spark.read.parquet("data/yellow_tripdata_2023-03.parquet")

    # Ensure same columns for all three (use Jan as reference)
    common_cols = df_jan.columns
    df_feb = df_feb.select(common_cols)
    df_mar = df_mar.select(common_cols)

    # Normalize passenger_count type
    df_jan = df_jan.withColumn("passenger_count", col("passenger_count").cast("double"))
    df_feb = df_feb.withColumn("passenger_count", col("passenger_count").cast("double"))
    df_mar = df_mar.withColumn("passenger_count", col("passenger_count").cast("double"))

    # Union into single DataFrame
    df = df_jan.unionByName(df_feb).unionByName(df_mar)

    record_count = df.count()  # action triggers the load
    results["load_time"] = time.time() - start
    print(f" ✓ Loaded {record_count:,} records in {results['load_time']:.2f}s")

    # Cache the DataFrame in memory so later tests run faster
    df.cache()
    df.count()  # materialize cache

    # ---------------------------
    # Test 2: Aggregation
    # ---------------------------
    print("Test 2: Aggregation...")
    start = time.time()

    (
        df.groupBy("payment_type", "passenger_count")
        .agg(
            spark_count("*").alias("cnt"),
            avg("fare_amount").alias("avg_fare"),
            spark_sum("total_amount").alias("sum_total")
        )
        .collect()  # action
    )

    results["aggregation_time"] = time.time() - start
    print(f" ✓ Completed in {results['aggregation_time']:.2f}s")

    # ---------------------------
    # Test 3: Filter & Sort
    # ---------------------------
    print("Test 3: Filter & Sort...")
    start = time.time()

    (
        df.filter(col("fare_amount") > 50)
        .orderBy(desc("fare_amount"))
        .limit(1000)
        .collect()  # action
    )

    results["filter_time"] = time.time() - start
    print(f" ✓ Completed in {results['filter_time']:.2f}s")

    # ---------------------------
    # Test 4: Join
    # ---------------------------
    print("Test 4: Join...")
    start = time.time()

    df_sample = df.sample(0.01)
    (
        df_sample.alias("a")
        .join(df_sample.alias("b"),
              col("a.passenger_count") == col("b.passenger_count"))
        .count()  # action
    )

    results["join_time"] = time.time() - start
    print(f" ✓ Completed in {results['join_time']:.2f}s")

    # Total time for this config
    results["total_time"] = sum(results.values())

    spark.stop()
    return results


# ===== MAIN: Benchmark Loop + Results =====

print("=" * 70)
print("SINGLE-NODE PERFORMANCE BENCHMARK")
print("=" * 70)

configs = [1, 2, 4]
all_results = {}

# Run benchmark for each core config
for cores in configs:
    all_results[cores] = run_benchmark(cores, f"Benchmark-{cores}Cores")
    time.sleep(3)  # small pause between runs

# Print comparison table
print("\n" + "=" * 85)
print("PERFORMANCE COMPARISON")
print("=" * 85)
print(f"{'Test':<20} {'1 Core':>12} {'2 Cores':>12} {'4 Cores':>12} {'Speedup':>12}")
print("-" * 85)

for test in ["load_time", "aggregation_time", "filter_time", "join_time", "total_time"]:
    times = [all_results[cores][test] for cores in configs]
    speedup = times[0] / times[2]  # 1-core time / 4-core time
    print(
        f"{test:<20} "
        f"{times[0]:>10.2f}s {times[1]:>10.2f}s {times[2]:>10.2f}s {speedup:>11.2f}x"
    )

print("=" * 85)

# Save results to file
with open("output/single_node_results.txt", "w") as f:
    f.write("Single Node Benchmark Results\n")
    f.write("=" * 50 + "\n\n")
    for cores in configs:
        f.write(f"{cores} Core(s):\n")
        for test, val in all_results[cores].items():
            f.write(f"  {test}: {val:.2f}s\n")
        f.write("\n")

print("\nResults saved to output/single_node_results.txt")
