from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, avg, sum, round, desc,
    rank, current_timestamp, countDistinct,
    first, when, count as cnt
)
from pyspark.sql.window import Window

# ─── Config ────────────────────────────────────────────────────────────────────
SILVER_PATH          = "./data/silver/social_events"
GOLD_HASHTAGS        = "./data/gold/top_hashtags"
GOLD_USER_ENGAGEMENT = "./data/gold/user_engagement"
GOLD_HOURLY_TRENDS   = "./data/gold/hourly_trends"

# ─── Spark Session ─────────────────────────────────────────────────────────────
def create_spark_session():
    return (
        SparkSession.builder
        .appName("SocialPulse-Silver-to-Gold")
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

# ─── Gold Table 1: Top Hashtags ────────────────────────────────────────────────
def build_top_hashtags(df):
    print("🏷️  Building top_hashtags...")

    posts = df.filter(col("event_type") == "post")

    hashtags_df = (
        posts
        .select(
            explode(col("hashtags")).alias("hashtag"),
            col("likes"),
            col("shares"),
            col("comments"),
            col("engagement_score"),
            col("is_viral"),
        )
        .groupBy("hashtag")
        .agg(
            cnt("*").alias("total_mentions"),
            round(avg("likes"), 1).alias("avg_likes"),
            round(avg("shares"), 1).alias("avg_shares"),
            round(avg("comments"), 1).alias("avg_comments"),
            round(avg("engagement_score"), 1).alias("avg_engagement"),
            sum(col("is_viral").cast("int")).alias("viral_count"),
        )
        .withColumn("rank",
            rank().over(Window.orderBy(desc("total_mentions")))
        )
        .withColumn("processed_at", current_timestamp())
        .orderBy("rank")
    )

    hashtags_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_HASHTAGS)

    n = hashtags_df.count()
    print(f"   ✅ top_hashtags: {n:,} hashtags ranked")
    return hashtags_df

# ─── Gold Table 2: User Engagement Scores ─────────────────────────────────────
def build_user_engagement(df):
    print("👤 Building user_engagement...")

    engagement_df = (
        df
        .groupBy("user_id", "username")
        .agg(
            cnt("*").alias("total_events"),
            cnt(when(col("event_type") == "post", 1)).alias("total_posts"),
            cnt(when(col("event_type") == "like", 1)).alias("total_likes"),
            cnt(when(col("event_type") == "share", 1)).alias("total_shares"),
            cnt(when(col("event_type") == "comment", 1)).alias("total_comments"),
            round(sum("engagement_score"), 1).alias("total_engagement_score"),
            round(avg("engagement_score"), 1).alias("avg_engagement_score"),
            cnt(when(col("is_viral") == True, 1)).alias("viral_posts"),
            countDistinct("device").alias("devices_used"),
            first("location").alias("primary_location"),
        )
        .withColumn("user_rank",
            rank().over(Window.orderBy(desc("total_engagement_score")))
        )
        .withColumn("processed_at", current_timestamp())
        .orderBy("user_rank")
    )

    engagement_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_USER_ENGAGEMENT)

    n = engagement_df.count()
    print(f"   ✅ user_engagement: {n:,} users scored")
    return engagement_df

# ─── Gold Table 3: Hourly Trends ──────────────────────────────────────────────
def build_hourly_trends(df):
    print("📈 Building hourly_trends...")

    trends_df = (
        df
        .groupBy("hour_of_day", "event_type")
        .agg(
            cnt("*").alias("event_count"),
            round(avg("engagement_score"), 1).alias("avg_engagement"),
            round(avg("likes"), 1).alias("avg_likes"),
            cnt(when(col("is_viral") == True, 1)).alias("viral_count"),
        )
        .withColumn("processed_at", current_timestamp())
        .orderBy("hour_of_day", "event_type")
    )

    trends_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_HOURLY_TRENDS)

    n = trends_df.count()
    print(f"   ✅ hourly_trends: {n:,} rows")
    return trends_df

# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("\n🚀 Starting Silver → Gold job...\n")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.format("delta").load(SILVER_PATH)
    print(f"📖 Silver events loaded: {silver_df.count():,}\n")

    hashtags_df   = build_top_hashtags(silver_df)
    engagement_df = build_user_engagement(silver_df)
    trends_df     = build_hourly_trends(silver_df)

    print("\n── Top 10 Trending Hashtags ──")
    hashtags_df.select("rank", "hashtag", "total_mentions", "avg_engagement", "viral_count") \
        .show(10)

    print("── Top 10 Users by Engagement ──")
    engagement_df.select("user_rank", "username", "total_posts", "total_engagement_score", "viral_posts") \
        .show(10)

    print("── Hourly Event Distribution ──")
    trends_df.groupBy("hour_of_day") \
        .agg(sum("event_count").alias("total_events")) \
        .orderBy("hour_of_day") \
        .show(24)

    print("\n✅ Silver → Gold complete!\n")
    spark.stop()

if __name__ == "__main__":
    main()