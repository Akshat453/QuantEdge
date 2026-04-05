"""
=================================================================
Spark Structured Streaming — Real-Time Stock Analytics
=================================================================
What this does:
  1. Connects to Kafka topic "stock-data" as a consumer
  2. Reads JSON messages in real-time (micro-batches)
  3. Parses each message into a structured DataFrame
  4. Computes analytics:
     - 5-period Moving Average of close price
     - Price Change % = ((close - open) / open) * 100
     - Volatility = (high - low) / open * 100
     - Volume trend (running sum per symbol)
  5. Prints results to console (for testing)
  6. Writes processed data to Parquet files in data/processed/

Run INSIDE the Spark Docker container:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
    spark/spark_stream.py
=================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, avg, sum as spark_sum,
    round as spark_round, current_timestamp, lit,
    to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType
)
from pyspark.sql.window import Window

# =================================================================
# 1. CREATE SPARK SESSION
# =================================================================
# "master('local[*]')" means use all available CPU cores on this machine
# "spark.sql.streaming.forceDeleteTempCheckpointLocation" cleans up
#   leftover checkpoints automatically on restart

spark = SparkSession.builder \
    .appName("StockMarketStreaming") \
    .master("local[*]") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# Reduce Spark's verbose logging to only show warnings and errors
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(" Spark Session created successfully!")
print("=" * 60)

# =================================================================
# 2. DEFINE THE SCHEMA FOR INCOMING JSON MESSAGES
# =================================================================
# This must match the JSON structure sent by producer.py:
# {"symbol": "AAPL", "open": 198.5, "high": 199.0, ...}

stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("timestamp", StringType(), True)
])

# =================================================================
# 3. READ STREAMING DATA FROM KAFKA
# =================================================================
# "kafka.bootstrap.servers" — the address of the Kafka broker.
#   Inside Docker on Mac, "host.docker.internal:9092" reaches the
#   host machine's port 9092 (where Kafka is exposed).
# "subscribe" — the Kafka topic to read from.
# "startingOffsets" — "earliest" means read all messages from the
#   beginning (useful for testing; in production you'd use "latest").

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "stock-data") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

print("[✓] Connected to Kafka topic 'stock-data'")

# =================================================================
# 4. PARSE JSON MESSAGES
# =================================================================
# Kafka sends messages as raw bytes in a "value" column.
# We cast bytes → string → parse JSON using our schema.

parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), stock_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

print("[✓] JSON schema applied to stream")

# =================================================================
# 5. COMPUTE ANALYTICS
# =================================================================
# These calculations run on EVERY micro-batch automatically.
#
# a) Price Change %  = ((close - open) / open) * 100
# b) Volatility      = ((high - low) / open) * 100
#
# NOTE: Moving Average and Volume Trend (window functions)
# are computed using stateful processing in the foreachBatch function
# below, since window() over streaming DataFrames requires special
# handling.

analytics_df = parsed_df \
    .withColumn(
        "price_change_pct",
        spark_round(((col("close") - col("open")) / col("open")) * 100, 2)
    ) \
    .withColumn(
        "volatility",
        spark_round(((col("high") - col("low")) / col("open")) * 100, 2)
    ) \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("date", to_date(col("timestamp")))

print("[✓] Analytics columns added (price_change_pct, volatility)")


# =================================================================
# 6. BATCH PROCESSING FUNCTION
# =================================================================
# foreachBatch lets us treat each micro-batch as a static DataFrame,
# which allows us to use window functions (like moving average).

def process_batch(batch_df, batch_id):
    """
    Called once per micro-batch.
    Adds moving average and volume trend, then:
      - Prints to console
      - Writes to Parquet
    """
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] Empty — no new data")
        return

    # Define a window: partition by stock symbol, ordered by timestamp
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")

    # 5-period Moving Average of close price
    # Uses rows between -4 (4 rows before) and 0 (current row) = 5 rows total
    window_avg = Window.partitionBy("symbol").orderBy("timestamp") \
        .rowsBetween(-4, 0)

    enriched_df = batch_df \
        .withColumn(
            "moving_avg_5",
            spark_round(avg("close").over(window_avg), 2)
        ) \
        .withColumn(
            "volume_cumulative",
            spark_sum("volume").over(window_spec)
        )

    # Count rows in this batch
    count = enriched_df.count()
    print(f"\n{'=' * 60}")
    print(f" Batch {batch_id} — {count} records processed")
    print(f"{'=' * 60}")

    # Show a sample in the console
    enriched_df.select(
        "symbol", "close", "price_change_pct", "volatility",
        "moving_avg_5", "volume_cumulative", "timestamp"
    ).show(10, truncate=False)

    # Write to Parquet — partitioned by symbol AND date for efficient querying
    enriched_df.write \
        .mode("append") \
        .partitionBy("symbol", "date") \
        .parquet("/opt/spark/work/data/processed")

    print(f"[✓] Batch {batch_id} written to data/processed/")


# =================================================================
# 7. START THE STREAMING QUERY
# =================================================================
# - foreachBatch: calls process_batch() on each micro-batch
# - trigger(processingTime="10 seconds"): processes a batch every 10s
# - checkpointLocation: stores progress so Spark can resume after crashes

print("\n[▶] Starting streaming query...")
print("[▶] Writing Parquet to: data/processed/")
print("[▶] Checkpoint at: data/checkpoint/")
print("[▶] Processing every 10 seconds")
print("[▶] Press Ctrl+C to stop\n")

query = analytics_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/opt/spark/work/data/checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

# Block until the query is stopped (Ctrl+C)
query.awaitTermination()
