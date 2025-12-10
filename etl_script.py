import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import (
    col, when, to_timestamp, date_format, 
    hour as spark_hour, count, countDistinct, sum as spark_sum,
    round as spark_round
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# ============================================================================
# INITIALIZATION
# ============================================================================

args = getResolvedOptions(
    sys.argv, 
    ['JOB_NAME', 'source_database', 'source_table', 'silver_path', 'gold_path']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Starting ETL job: {args['JOB_NAME']}")
print(f"Source: {args['source_database']}.{args['source_table']}")
print(f"Silver output: {args['silver_path']}")
print(f"Gold output: {args['gold_path']}")

# ============================================================================
# BRONZE -> SILVER: READ AND TRANSFORM RAW DATA
# ============================================================================

print("Reading from Bronze layer (Glue Catalog)...")
source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args['source_database'],
    table_name=args['source_table'],
    transformation_ctx="source_dyf"
)

print(f"Records read from Bronze: {source_dyf.count()}")

# Convert to DataFrame for easier transformations
df = source_dyf.toDF()

# Check if DataFrame is empty (job bookmarks may have filtered all files)
if df.count() == 0:
    print("No new records to process (job bookmarks working correctly)")
    print("Job completed successfully with 0 records processed")
else:
    print("Applying transformations...")

    # 1. DATA QUALITY: Filter out invalid records
    df_clean = df.filter(
        col("timestamp").isNotNull() &
        col("user_id").isNotNull() &
        col("session_id").isNotNull() &
        col("event_type").isNotNull()
    )
    
    records_before = df.count()
    records_after = df_clean.count()
    print(f"Data quality check: {records_before - records_after} invalid records removed")
    
    # 2. PARSE TIMESTAMP: Convert ISO 8601 string to proper timestamp
    df_clean = df_clean.withColumn(
        "timestamp", 
        to_timestamp(col("timestamp"))
    )
    
    # 3. ADD REVENUE: Calculate revenue for purchase events (price × quantity)
    df_clean = df_clean.withColumn(
        "revenue",
        when(
            col("event_type") == "purchase",
            spark_round(col("price") * col("quantity"), 2)
        ).otherwise(0.0)
    )
    
    # 4. ADD DATE FIELDS: Make queries easier (even though we have partitions)
    df_clean = df_clean.withColumn("event_date", date_format(col("timestamp"), "yyyy-MM-dd"))
    df_clean = df_clean.withColumn("event_hour", spark_hour(col("timestamp")))
    
    # Select and reorder columns for clarity
    silver_df = df_clean.select(
        "timestamp",
        "event_date",
        "event_hour",
        "user_id",
        "session_id",
        "event_type",
        "product_id",
        "quantity",
        "price",
        "revenue",
        "category",
        "search_query",
        # Partition columns
        "year",
        "month",
        "day",
        "hour"
    )
    
    print(f"Silver layer records: {silver_df.count()}")
    
    # Convert back to DynamicFrame and write to Silver
    print("Writing to Silver layer (enriched events)...")
    silver_dyf = DynamicFrame.fromDF(silver_df, glueContext, "silver_dyf")
    
    glueContext.write_dynamic_frame.from_options(
        frame=silver_dyf,
        connection_type="s3",
        connection_options={
            "path": args['silver_path'],
            "partitionKeys": ["year", "month", "day", "hour"]
        },
        format="parquet",
        format_options={
            "compression": "snappy"
        },
        transformation_ctx="silver_sink"
    )
    
    print("Silver layer write complete")

    # ============================================================================
    # SILVER -> GOLD: CREATE AGGREGATED ANALYTICS TABLES
    # ============================================================================
    
    print("\nCreating Gold layer aggregations...")
    
    # ============================================================================
    # GOLD TABLE 1: Product Conversion Funnel
    # Pre-aggregate for Query 1: view → cart → purchase rates by product
    # ============================================================================
    
    print("Creating product_funnel table...")
    product_funnel = silver_df.filter(
        col("product_id").isNotNull()
    ).groupBy("product_id", "category").agg(
        count(when(col("event_type") == "page_view", 1)).alias("view_count"),
        count(when(col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        count(when(col("event_type") == "purchase", 1)).alias("purchase_count")
    ).withColumn(
        "view_to_cart_rate",
        spark_round(
            when(col("view_count") > 0, col("add_to_cart_count") / col("view_count") * 100)
            .otherwise(0.0),
            2
        )
    ).withColumn(
        "cart_to_purchase_rate",
        spark_round(
            when(col("add_to_cart_count") > 0, col("purchase_count") / col("add_to_cart_count") * 100)
            .otherwise(0.0),
            2
        )
    ).withColumn(
        "view_to_purchase_rate",
        spark_round(
            when(col("view_count") > 0, col("purchase_count") / col("view_count") * 100)
            .otherwise(0.0),
            2
        )
    )
    
    product_funnel_dyf = DynamicFrame.fromDF(product_funnel, glueContext, "product_funnel")
    glueContext.write_dynamic_frame.from_options(
        frame=product_funnel_dyf,
        connection_type="s3",
        connection_options={"path": f"{args['gold_path']}/product_funnel/"},
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="product_funnel_sink"
    )
    print(f"  product_funnel: {product_funnel.count()} products")

    # ============================================================================
    # GOLD TABLE 2: Hourly Revenue
    # Pre-aggregate for Query 2: Total revenue by hour
    # ============================================================================
    
    print("Creating hourly_revenue table...")
    hourly_revenue = silver_df.filter(
        col("event_type") == "purchase"
    ).groupBy(
        "event_date", 
        "event_hour",
        "year",
        "month",
        "day",
        "hour"
    ).agg(
        spark_sum("revenue").alias("total_revenue"),
        count("*").alias("purchase_count"),
        spark_sum("quantity").alias("total_items_sold")
    ).withColumn(
        "avg_order_value",
        spark_round(col("total_revenue") / col("purchase_count"), 2)
    ).withColumn(
        "total_revenue",
        spark_round(col("total_revenue"), 2)
    )
    
    hourly_revenue_dyf = DynamicFrame.fromDF(hourly_revenue, glueContext, "hourly_revenue")
    glueContext.write_dynamic_frame.from_options(
        frame=hourly_revenue_dyf,
        connection_type="s3",
        connection_options={
            "path": f"{args['gold_path']}/hourly_revenue/",
            "partitionKeys": ["year", "month", "day"]
        },
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="hourly_revenue_sink"
    )
    print(f"  hourly_revenue: {hourly_revenue.count()} hourly aggregations")

    # ============================================================================
    # GOLD TABLE 3: Product Popularity (Top Products)
    # Pre-aggregate for Query 3: Most viewed products
    # ============================================================================
    
    print("Creating product_popularity table...")
    product_popularity = silver_df.filter(
        (col("event_type") == "page_view") & 
        col("product_id").isNotNull()
    ).groupBy("product_id", "category").agg(
        count("*").alias("view_count"),
        countDistinct("user_id").alias("unique_viewers")
    ).orderBy(col("view_count").desc())
    
    product_popularity_dyf = DynamicFrame.fromDF(product_popularity, glueContext, "product_popularity")
    glueContext.write_dynamic_frame.from_options(
        frame=product_popularity_dyf,
        connection_type="s3",
        connection_options={"path": f"{args['gold_path']}/product_popularity/"},
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="product_popularity_sink"
    )
    print(f"  product_popularity: {product_popularity.count()} products")

    # ============================================================================
    # GOLD TABLE 4: Daily Category Performance
    # Pre-aggregate for Query 4: Event counts by category and day
    # ============================================================================
    
    print("Creating category_daily_performance table...")
    category_performance = silver_df.filter(
        col("category").isNotNull()
    ).groupBy(
        "event_date",
        "category",
        "year",
        "month",
        "day"
    ).agg(
        count("*").alias("total_events"),
        count(when(col("event_type") == "page_view", 1)).alias("page_views"),
        count(when(col("event_type") == "add_to_cart", 1)).alias("add_to_carts"),
        count(when(col("event_type") == "purchase", 1)).alias("purchases"),
        spark_sum("revenue").alias("total_revenue")
    ).withColumn(
        "total_revenue",
        spark_round(col("total_revenue"), 2)
    )
    
    category_performance_dyf = DynamicFrame.fromDF(category_performance, glueContext, "category_performance")
    glueContext.write_dynamic_frame.from_options(
        frame=category_performance_dyf,
        connection_type="s3",
        connection_options={
            "path": f"{args['gold_path']}/category_daily_performance/",
            "partitionKeys": ["year", "month", "day"]
        },
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="category_performance_sink"
    )
    print(f"  category_daily_performance: {category_performance.count()} category-day combinations")

    # ============================================================================
    # GOLD TABLE 5: Daily User Activity
    # Pre-aggregate for Query 5: Unique users and sessions per day
    # ============================================================================
    
    print("Creating daily_user_activity table...")
    daily_activity = silver_df.groupBy(
        "event_date",
        "year",
        "month",
        "day"
    ).agg(
        countDistinct("user_id").alias("unique_users"),
        countDistinct("session_id").alias("unique_sessions"),
        count("*").alias("total_events")
    ).withColumn(
        "events_per_user",
        spark_round(col("total_events") / col("unique_users"), 2)
    ).withColumn(
        "sessions_per_user",
        spark_round(col("unique_sessions") / col("unique_users"), 2)
    )
    
    daily_activity_dyf = DynamicFrame.fromDF(daily_activity, glueContext, "daily_activity")
    glueContext.write_dynamic_frame.from_options(
        frame=daily_activity_dyf,
        connection_type="s3",
        connection_options={
            "path": f"{args['gold_path']}/daily_user_activity/",
            "partitionKeys": ["year", "month", "day"]
        },
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="daily_activity_sink"
    )
    print(f"  daily_user_activity: {daily_activity.count()} days")
    
    # ============================================================================
    # JOB COMPLETION
    # ============================================================================
    
    print("\nETL job completed successfully!")
    print("Summary:")
    print(f"  - Silver layer: Enriched events with revenue calculation")
    print(f"  - Gold layer: 5 pre-aggregated tables for optimized queries")
    print(f"    1. product_funnel (conversion rates)")
    print(f"    2. hourly_revenue (revenue by hour)")
    print(f"    3. product_popularity (top products)")
    print(f"    4. category_daily_performance (category metrics)")
    print(f"    5. daily_user_activity (user/session counts)")

job.commit()
