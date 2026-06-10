from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, length,
    lower, trim, size, hour, dayofweek,
    current_timestamp, count, row_number
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ─── Config ────────────────────────────────────────────────────────────────────
BRONZE_PATH = "./data/bronze/social_events"
SILVER_PATH = "./data/silver/social_events"

# ─── Spark Session ─────────────────────────────────────────────────────────────
def create_spark_session():
    return (
        SparkSession.builder
        .appName("SocialPulse-Bronze-to-Silver")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .master("local[*]")
        .getOrCreate()
    )

# ─── Read Bronze ───────────────────────────────────────────────────────────────
def read_bronze(spark):
    print("📖 Reading Bronze layer...")
    df = spark.read.format("delta").load(BRONZE_PATH)
    print(f"   Raw count: {df.count():,}")
    return df

# ─── Clean & Enrich ────────────────────────────────────────────────────────────
def clean_and_enrich(df):
    print("🧹 Cleaning and enriching...")

    # 1. Deduplicate by event_id (keep latest)
    window = Window.partitionBy("event_id").orderBy(col("ingested_at").desc())
    df = (
        df.withColumn("row_num", row_number().over(window))
          .filter(col("row_num") == 1)
          .drop("row_num")
    )

    # 2. Drop nulls on critical fields
    df = df.dropna(subset=["event_id", "event_type", "user_id", "timestamp"])

    # 3. Standardize strings
    df = (
        df
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn("device",     lower(trim(col("device"))))
        .withColumn("username",   lower(trim(col("username"))))
    )

    # 4. Filter bad data
    df = (
        df
        .filter(length(col("event_id")) > 0)
        .filter(col("event_type").isin(
            "post", "like", "share", "comment", "repost"
        ))
        .filter(col("likes")    >= 0)
        .filter(col("shares")   >= 0)
        .filter(col("comments") >= 0)
    )

    # 5. Derive useful columns
    df = (
        df
        .withColumn("hour_of_day",    hour(col("timestamp")))
        .withColumn("day_of_week",    dayofweek(col("timestamp")))
        .withColumn("hashtag_count",  size(col("hashtags")))
        .withColumn("is_viral",
            when(
                (col("event_type") == "post") &
                (col("likes") > 5000) &
                (col("shares") > 1000),
                True
            ).otherwise(False)
        )
        .withColumn("engagement_score",
            when(col("event_type") == "post",
                col("likes") * 1.0 +
                col("shares") * 2.0 +
                col("comments") * 1.5
            ).otherwise(0)
        )
        .withColumn("processed_at", current_timestamp())
    )

    return df

# ─── Write Silver ──────────────────────────────────────────────────────────────
def write_silver(df):
    print("💾 Writing Silver layer...")
    count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_type")
        .save(SILVER_PATH)
    )

    print(f"   ✅ Silver written: {count:,} clean events")
    return count

# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("\n🚀 Starting Bronze → Silver job...\n")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = read_bronze(spark)
    silver_df = clean_and_enrich(bronze_df)
    silver_count = write_silver(silver_df)

    # Quick summary
    print("\n── Silver Layer Summary ──")
    spark.read.format("delta").load(SILVER_PATH) \
        .groupBy("event_type") \
        .count() \
        .orderBy("count", ascending=False) \
        .show()

    print(f"\n✅ Bronze → Silver complete! {silver_count:,} events processed.\n")
    spark.stop()

if __name__ == "__main__":
    main()
