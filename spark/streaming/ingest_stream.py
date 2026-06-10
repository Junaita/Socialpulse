from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, explode, window,
    count, avg, to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, ArrayType, TimestampType
)

# ─── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
KAFKA_TOPIC    = "social-events"
BRONZE_PATH    = "./data/bronze/social_events"
TRENDING_PATH  = "./data/bronze/trending_hashtags"
CHECKPOINT_EVENTS   = "./data/checkpoints/social_events"
CHECKPOINT_TRENDING = "./data/checkpoints/trending_hashtags"

# ─── Schema ────────────────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(),           True),
    StructField("event_type", StringType(),           True),
    StructField("user_id",    StringType(),           True),
    StructField("username",   StringType(),           True),
    StructField("content",    StringType(),           True),
    StructField("hashtags",   ArrayType(StringType()), True),
    StructField("likes",      IntegerType(),          True),
    StructField("shares",     IntegerType(),          True),
    StructField("comments",   IntegerType(),          True),
    StructField("location",   StringType(),           True),
    StructField("device",     StringType(),           True),
    StructField("timestamp",  StringType(),           True),
])

# ─── Spark Session ─────────────────────────────────────────────────────────────
def create_spark_session():
    return (
        SparkSession.builder
        .appName("SocialPulse-Streaming")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation", "./data/checkpoints")
        .config("spark.sql.shuffle.partitions", "4")   # low for local dev
        .master("local[*]")
        .getOrCreate()
    )

# ─── Read from Kafka ───────────────────────────────────────────────────────────
def read_kafka_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

# ─── Parse Events ──────────────────────────────────────────────────────────────
def parse_events(raw_df):
    return (
        raw_df
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("ingested_at", current_timestamp())
    )

# ─── Stream 1: Write Bronze (raw events → Delta Lake) ─────────────────────────
def write_bronze(events_df):
    return (
        events_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_EVENTS)
        .partitionBy("event_type")          # partition by type for faster queries
        .start(BRONZE_PATH)
    )

# ─── Stream 2: Trending Hashtags (5-min window) ────────────────────────────────
def write_trending_hashtags(events_df):
    trending = (
        events_df
        .filter(col("event_type") == "post")       # only posts have hashtags
        .select(
            col("timestamp"),
            explode(col("hashtags")).alias("hashtag"),
            col("likes"),
            col("shares"),
        )
        .withWatermark("timestamp", "10 minutes")  # handle late data
        .groupBy(
            window(col("timestamp"), "5 minutes"),  # 5-min tumbling window
            col("hashtag")
        )
        .agg(
            count("*").alias("mention_count"),
            avg("likes").alias("avg_likes"),
            avg("shares").alias("avg_shares"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("hashtag"),
            col("mention_count"),
            col("avg_likes"),
            col("avg_shares"),
        )
    )

    return (
        trending.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_TRENDING)
        .start(TRENDING_PATH)
    )

# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("🚀 Starting SocialPulse Spark Streaming...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # reduce noise

    # Read raw Kafka stream
    raw_df    = read_kafka_stream(spark)
    events_df = parse_events(raw_df)

    # Start both streams in parallel
    bronze_query   = write_bronze(events_df)
    trending_query = write_trending_hashtags(events_df)

    print("✅ Streaming started!")
    print(f"   📁 Bronze events  → {BRONZE_PATH}")
    print(f"   📁 Trending       → {TRENDING_PATH}")
    print("   Waiting for data... (Ctrl+C to stop)\n")

    # Wait for both streams
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
