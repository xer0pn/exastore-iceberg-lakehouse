"""
AWS Glue Job: Gold Layer
Reads from Silver Iceberg tables (S3 Tables), writes aggregated business
metrics to Gold Iceberg tables, also on S3 Tables (managed Iceberg).

Your task: Complete the TODOs to build gold-layer aggregations.
  1. customer_analytics  — monthly customer/revenue/segment aggregates
  2. realtime_metrics    — hourly event/traffic/revenue aggregates

Key concepts:
  - Single s3tablescatalog handles both silver (read) and gold (write) namespaces
  - Same SparkSession.builder + writeTo().create() pattern as silver
  - Gold namespace created via Glue API (boto3), not Spark SQL
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'database_name', 'data_lake_bucket',
    'gold_table_bucket_arn', 'aws_region'
])

BUCKET = args['data_lake_bucket']
GOLD_TABLE_BUCKET_ARN = args['gold_table_bucket_arn']
AWS_REGION = args.get('aws_region', 'us-east-1')

# Parse the S3 Tables bucket ARN
arn_parts = GOLD_TABLE_BUCKET_ARN.split(":")
ACCOUNT_ID = arn_parts[4]
TABLE_BUCKET_NAME = arn_parts[5].split("/")[1]

DB_SILVER = "ecommerce_silver"   # Read source: silver namespace in S3 Tables
DB_GOLD = "ecommerce_gold"      # Write target: gold namespace in S3 Tables
S3T_CAT = "s3tablescatalog"     # Single catalog handles both silver and gold

sc = SparkContext()
glueContext = GlueContext(sc)

# TODO 1: Configure Spark with the S3 Tables catalog using SparkSession.builder.
# Same pattern as the silver layer — register s3tablescatalog with GlueCatalog + glue.id.
#
# spark = SparkSession.builder \
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
#     .config(f"spark.sql.catalog.{S3T_CAT}", "org.apache.iceberg.spark.SparkCatalog") \
#     .config(f"spark.sql.catalog.{S3T_CAT}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
#     .config(f"spark.sql.catalog.{S3T_CAT}.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}") \
#     .config(f"spark.sql.catalog.{S3T_CAT}.warehouse", f"s3://{BUCKET}/gold-warehouse/") \
#     .getOrCreate()

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{S3T_CAT}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{S3T_CAT}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{S3T_CAT}.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}") \
    .config(f"spark.sql.catalog.{S3T_CAT}.warehouse", f"s3://{BUCKET}/gold-warehouse/") \
    .getOrCreate()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info(f"S3 Tables catalog: {S3T_CAT} → {TABLE_BUCKET_NAME}")
logger.info(f"Reading silver from: {S3T_CAT}.{DB_SILVER}")
logger.info(f"Writing gold to: {S3T_CAT}.{DB_GOLD}")

# TODO 2: Create the gold namespace in S3 Tables if it doesn't exist.
# Same boto3 pattern as the silver layer.
#
# import boto3
# glue_client = boto3.client('glue', region_name=AWS_REGION)
# try:
#     glue_client.create_database(
#         CatalogId=f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}",
#         DatabaseInput={'Name': DB_GOLD}
#     )
# except Exception as e:
#     logger.info(f"Namespace already exists: {e}")
import boto3
glue_client = boto3.client('glue', region_name=AWS_REGION)
try:
    glue_client.create_database(
        CatalogId=f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}",
        DatabaseInput={'Name': DB_GOLD}
    )
except Exception as e:
    logger.info(f"Namespace already exists or is already available: {e}")


def create_gold_table(name, query):
    """Query silver, write results to S3 Tables gold namespace."""
    fqn = f"{S3T_CAT}.{DB_GOLD}.{name}"

    # TODO 3: Execute the query, drop existing table with PURGE, write with writeTo.
    # Same pattern as silver's create_silver_table.
    #
    # df = spark.sql(query)
    # logger.info(f"{name}: {df.count()} rows to write")
    # spark.sql(f"DROP TABLE IF EXISTS {fqn} PURGE")
    # df.writeTo(fqn).using("iceberg").tableProperty("format-version", "2").create()
    df = spark.sql(query)
    logger.info(f"{name}: {df.count()} rows to write")
    spark.sql(f"DROP TABLE IF EXISTS {fqn} PURGE")
    df.writeTo(fqn).using("iceberg").tableProperty("format-version", "2").create()


# ── 1. customer_analytics ────────────────────────────────────────────────────
# TODO 4: Create gold.customer_analytics by aggregating silver.order_details.
# Read from {S3T_CAT}.{DB_SILVER}.order_details
# GROUP BY date_trunc('month', order_date) AS analysis_month
# Aggregate:
#   - total_customers, new_customers (order_sequence=1), returning_customers
#   - total_orders, total_revenue, avg_order_value
#   - Orders per segment: new_customer_orders, repeat_customer_orders, loyal_customer_orders, vip_customer_orders
#   - Geographic: cities_served, states_served
#   - Revenue by segment: new_customer_revenue, vip_customer_revenue, loyal_customer_revenue
#   - avg_customer_lifetime_value
#   - gold_processed_at: current_timestamp()
create_gold_table("customer_analytics", f"""
    SELECT date_trunc('month', order_date) AS analysis_month,
           COUNT(DISTINCT user_id) AS total_customers,
           COUNT(DISTINCT CASE WHEN customer_order_sequence = 1 THEN user_id END) AS new_customers,
           COUNT(DISTINCT CASE WHEN customer_order_sequence > 1 THEN user_id END) AS returning_customers,
           COUNT(*) AS total_orders, SUM(total_amount) AS total_revenue,
           AVG(total_amount) AS avg_order_value,
           SUM(CASE WHEN customer_segment = 'New' THEN 1 ELSE 0 END) AS new_customer_orders,
           SUM(CASE WHEN customer_segment = 'Repeat' THEN 1 ELSE 0 END) AS repeat_customer_orders,
           SUM(CASE WHEN customer_segment = 'Loyal' THEN 1 ELSE 0 END) AS loyal_customer_orders,
           SUM(CASE WHEN customer_segment = 'VIP' THEN 1 ELSE 0 END) AS vip_customer_orders,
           COUNT(DISTINCT city) AS cities_served, COUNT(DISTINCT state) AS states_served,
           SUM(CASE WHEN customer_segment = 'New' THEN total_amount ELSE 0 END) AS new_customer_revenue,
           SUM(CASE WHEN customer_segment = 'VIP' THEN total_amount ELSE 0 END) AS vip_customer_revenue,
           SUM(CASE WHEN customer_segment = 'Loyal' THEN total_amount ELSE 0 END) AS loyal_customer_revenue,
           AVG(customer_lifetime_value) AS avg_customer_lifetime_value,
           current_timestamp() AS gold_processed_at
    FROM {S3T_CAT}.{DB_SILVER}.order_details
    GROUP BY date_trunc('month', order_date)
""")

# ── 2. realtime_metrics ──────────────────────────────────────────────────────
# TODO 5: Create gold.realtime_metrics by aggregating silver.enriched_events.
# Read from {S3T_CAT}.{DB_SILVER}.enriched_events
# GROUP BY date_trunc('hour', event_timestamp) AS metric_hour
# Aggregate:
#   - Traffic: total_events, unique_users, unique_sessions
#   - Event breakdown: page_views, add_to_carts, purchases, clicks
#   - Revenue: hourly_revenue (sum revenue for purchases), avg_purchase_value
#   - Traffic sources: google_traffic, facebook_traffic, direct_traffic
#   - categories_viewed (distinct product_category)
#   - gold_processed_at: current_timestamp()
# Filter: WHERE event_date >= DATE '2024-01-01'
create_gold_table("realtime_metrics", f"""
    SELECT date_trunc('hour', event_timestamp) AS metric_hour,
           COUNT(*) AS total_events, COUNT(DISTINCT user_id) AS unique_users,
           COUNT(DISTINCT session_id) AS unique_sessions,
           SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
           SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_carts,
           SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
           SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
           SUM(CASE WHEN event_type = 'purchase' THEN COALESCE(revenue, 0.0) ELSE 0.0 END) AS hourly_revenue,
           AVG(CASE WHEN event_type = 'purchase' THEN revenue END) AS avg_purchase_value,
           SUM(CASE WHEN lower(source) = 'google' THEN 1 ELSE 0 END) AS google_traffic,
           SUM(CASE WHEN lower(source) = 'facebook' THEN 1 ELSE 0 END) AS facebook_traffic,
           SUM(CASE WHEN source IS NULL OR lower(source) = 'direct' THEN 1 ELSE 0 END) AS direct_traffic,
           COUNT(DISTINCT product_category) AS categories_viewed,
           current_timestamp() AS gold_processed_at
    FROM {S3T_CAT}.{DB_SILVER}.enriched_events
    WHERE event_date >= DATE '2024-01-01'
    GROUP BY date_trunc('hour', event_timestamp)
""")

logger.info("Gold layer complete — tables written to S3 Tables")
job.commit()
