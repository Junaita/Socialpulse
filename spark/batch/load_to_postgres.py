import psycopg2
from pyspark.sql import SparkSession

# ─── Config ────────────────────────────────────────────────────────────────────
GOLD_HASHTAGS        = "./data/gold/top_hashtags"
GOLD_USER_ENGAGEMENT = "./data/gold/user_engagement"
GOLD_HOURLY_TRENDS   = "./data/gold/hourly_trends"

POSTGRES_URL  = "jdbc:postgresql://localhost:5432/socialpulse"
POSTGRES_PROPS = {
    "user":     "socialpulse",
    "password": "socialpulse123",
    "driver":   "org.postgresql.Driver",
}

# ─── Drop dbt views + raw tables with CASCADE ──────────────────────────────────
def drop_views_and_tables():
    print("🗑️  Dropping dbt views and raw tables (CASCADE)...")
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname="socialpulse",
        user="socialpulse", password="socialpulse123",
    )
    conn.autocommit = True
    cur = conn.cursor()

    statements = [
        # Drop dbt table mart first
        "DROP TABLE IF EXISTS mart_trending_topics CASCADE",
        # Drop dbt views
        "DROP VIEW IF EXISTS stg_top_hashtags CASCADE",
        "DROP VIEW IF EXISTS stg_user_engagement CASCADE",
        # Drop raw tables
        "DROP TABLE IF EXISTS raw_top_hashtags CASCADE",
        "DROP TABLE IF EXISTS raw_user_engagement CASCADE",
        "DROP TABLE IF EXISTS raw_hourly_trends CASCADE",
    ]
    for stmt in statements:
        cur.execute(stmt)
        print(f"   ✅ {stmt}")

    cur.close()
    conn.close()
    print("   Done.\n")

# ─── Spark Session ─────────────────────────────────────────────────────────────
def create_spark_session():
    return (
        SparkSession.builder
        .appName("SocialPulse-Load-Postgres")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .master("local[*]")
        .getOrCreate()
    )

# ─── Load table to Postgres ────────────────────────────────────────────────────
def load_to_postgres(df, table_name):
    print(f"💾 Loading {table_name} → Postgres...")
    n = df.count()
    df.write.jdbc(
        url=POSTGRES_URL,
        table=table_name,
        mode="overwrite",
        properties=POSTGRES_PROPS,
    )
    print(f"   ✅ {table_name}: {n:,} rows loaded")

# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("\n🚀 Loading Gold layer → Postgres...\n")

    # Step 1: Drop views/tables so overwrite works cleanly
    drop_views_and_tables()

    # Step 2: Load fresh data
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    hashtags_df   = spark.read.format("delta").load(GOLD_HASHTAGS)
    load_to_postgres(hashtags_df, "raw_top_hashtags")

    engagement_df = spark.read.format("delta").load(GOLD_USER_ENGAGEMENT)
    load_to_postgres(engagement_df, "raw_user_engagement")

    trends_df = spark.read.format("delta").load(GOLD_HOURLY_TRENDS)
    load_to_postgres(trends_df, "raw_hourly_trends")

    print("\n── Tables loaded into Postgres ──")
    print("   raw_top_hashtags")
    print("   raw_user_engagement")
    print("   raw_hourly_trends")
    print("\n✅ Done! Ready for dbt.\n")
    spark.stop()

if __name__ == "__main__":
    main()