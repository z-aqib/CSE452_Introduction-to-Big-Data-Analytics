from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, sum as spark_sum, count as spark_count
import time

def run_cluster_benchmark(cores, app_name):
    print("\n" + "=" * 70)
    print(f"CLUSTER BENCHMARK: {cores} Cores")
    print("=" * 70)

    # Connect to Spark standalone cluster
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://localhost:7077")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.cores.max", str(cores))
        .config("spark.sql.shuffle.partitions", str(cores * 4))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    results = {}

    # ---------------------------
    # Test 1: Load + cache
    # ---------------------------
    print("Test 1: Loading...")
    start = time.time()

    # [CHANGED] Load each month separately and normalize schema
    df_jan = spark.read.parquet("data/yellow_tripdata_2023-01.parquet")
    df_feb = spark.read.parquet("data/yellow_tripdata_2023-02.parquet")
    df_mar = spark.read.parquet("data/yellow_tripdata_2023-03.parquet")

    common_cols = df_jan.columns
    df_feb = df_feb.select(common_cols)
    df_mar = df_mar.select(common_cols)

    df_jan = df_jan.withColumn("passenger_count", col("passenger_count").cast("double"))
    df_feb = df_feb.withColumn("passenger_count", col("passenger_count").cast("double"))
    df_mar = df_mar.withColumn("passenger_count", col("passenger_count").cast("double"))

    df = df_jan.unionByName(df_feb).unionByName(df_mar)

    df.count()
    results["load_time"] = time.time() - start
    print(f" ✓ {results['load_time']:.2f}s")

    # Repartition across executors and cache
    df = df.repartition(cores * 4).cache()
    df.count()  # materialize cache

    # ---------------------------
    # Test 2: Aggregation
    # ---------------------------
    print("Test 2: Aggregation...")
    start = time.time()

    (
        df.groupBy("payment_type")
        .agg(
            spark_count("*").alias("cnt"),
            avg("fare_amount").alias("avg_fare")
        )
        .collect()
    )

    results["aggregation_time"] = time.time() - start
    print(f" ✓ {results['aggregation_time']:.2f}s")

    # ---------------------------
    # Test 3: Filter
    # ---------------------------
    print("Test 3: Filter...")
    start = time.time()

    df.filter(col("fare_amount") > 50).limit(10000).collect()

    results["filter_time"] = time.time() - start
    print(f" ✓ {results['filter_time']:.2f}s")

    # ---------------------------
    # Test 4: Join
    # ---------------------------
    print("Test 4: Join...")
    start = time.time()

    df_left = df.sample(0.02).repartition(cores, "passenger_count")
    df_right = df.sample(0.02).repartition(cores, "passenger_count")

    df_left.join(df_right, "passenger_count").count()

    results["join_time"] = time.time() - start
    print(f" ✓ {results['join_time']:.2f}s")

    results["total_time"] = sum(results.values())

    spark.stop()
    time.sleep(5)  # small pause between cluster runs
    return results


# ===== MAIN: Cluster Benchmark Loop + Results =====

print("=" * 70)
print("CLUSTER PERFORMANCE BENCHMARK")
print("=" * 70)

configurations = [2, 4, 6]
all_results = {}

for cores in configurations:
    all_results[cores] = run_cluster_benchmark(cores, f"Cluster-{cores}Cores")

# Print comparison table
print("\n" + "=" * 85)
print("CLUSTER COMPARISON")
print("=" * 85)
print(f"{'Test':<20} {'2 Cores':>12} {'4 Cores':>12} {'6 Cores':>12} {'Speedup':>12}")
print("-" * 85)

for test in ["load_time", "aggregation_time", "filter_time", "join_time", "total_time"]:
    times = [all_results[cores][test] for cores in configurations]
    speedup = times[0] / times[2]  # 2-core / 6-core
    print(
        f"{test:<20} "
        f"{times[0]:>10.2f}s {times[1]:>10.2f}s {times[2]:>10.2f}s {speedup:>11.2f}x"
    )

print("=" * 85)

# Save results
with open("output/cluster_results.txt", "w") as f:
    f.write("Cluster Benchmark Results\n")
    f.write("=" * 50 + "\n\n")
    for cores in configurations:
        f.write(f"{cores} Cores:\n")
        for test, val in all_results[cores].items():
            f.write(f"  {test}: {val:.2f}s\n")
        f.write("\n")

print("\nResults saved to output/cluster_results.txt")
