from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Schema of the JSON messages coming from Kafka
schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("status", StringType())
])

# SparkSession connected to the Spark standalone cluster
spark = (
    SparkSession.builder
    .appName("Activity4-Streaming")
    .master("spark://localhost:7077")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("Spark Structured Streaming - Kafka Integration")
print("=" * 70)
print("Reading from topic: sensor-data")

# --------------------------
# 1) Read stream from Kafka
# --------------------------
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor-data")
    .option("startingOffsets", "latest")
    .load()
)

# 'raw' schema: key (binary), value (binary), topic, partition, offset, timestamp, etc.

# ------------------------------------
# 2) Parse JSON from Kafka 'value'
# ------------------------------------
parsed = (
    raw.selectExpr("CAST(value AS STRING)")
       .select(from_json(col("value"), schema).alias("data"))
       .select("data.*")
       .withColumn("event_time", to_timestamp(col("timestamp")))
)

# ------------------------------------
# 3) Build "alerts" stream
#    - Filter high temp OR critical status
#    - Create an alert message string
# ------------------------------------
alerts = (
    parsed
    .filter((col("temperature") > 30) | (col("status") == "critical"))
    .select(col("sensor_id"), col("temperature"), col("status"))
    .withColumn(
        "alert_msg",
        concat(
            lit("ALERT: "), col("sensor_id"),
            lit(" - temp="), col("temperature"),
            lit(" status="), col("status")
        )
    )
)

# ------------------------------------
# 4) Windowed aggregation (5-second windows)
#    - Count how many 'critical' statuses per sensor_id
#    - Use watermark to bound late events
# ------------------------------------
windowed = (
    parsed
    .withWatermark("event_time", "5 seconds")
    .filter(col("status") == "critical")
    .groupBy(window(col("event_time"), "5 seconds"), col("sensor_id"))
    .count()
    .withColumnRenamed("count", "critical_count")
)

# ------------------------------------
# 5) Prepare alerts to be written back to Kafka
#    - key: sensor_id (string)
#    - value: JSON with sensor_id, temperature, status, alert_msg
# ------------------------------------
kafka_output = alerts.select(
    col("sensor_id").cast("string").alias("key"),
    to_json(struct("sensor_id", "temperature", "status", "alert_msg")).alias("value")
)

# Write alerts to Kafka topic "alerts"
query1 = (
    kafka_output.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "alerts")
    .option("checkpointLocation", "checkpoint/alerts")
    .start()
)

# Write windowed counts to console
query2 = (
    windowed.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .start()
)

print("Streaming queries active... Press Ctrl+C to stop")
spark.streams.awaitAnyTermination()
