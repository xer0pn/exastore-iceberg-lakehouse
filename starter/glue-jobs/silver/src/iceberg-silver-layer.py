"""
AWS Glue Job: Silver Layer
Reads from Bronze Iceberg tables (Glue Catalog on S3), writes enriched/joined
Silver tables to S3 Tables (managed Iceberg).

Your task: Complete the TODOs below to build 3 silver tables.
  1. order_details       — orders enriched with customer context + window functions
  2. enriched_events     — events enriched with user + product context
  3. product_performance — product-level aggregates from events data

Key concepts:
  - Dual catalogs: glue_catalog (read bronze from S3) + s3tablescatalog (write silver to S3 Tables)
  - SparkSession.builder configures the S3 Tables catalog at startup
  - writeTo().create() writes DataFrames to S3 Tables (not CTAS SQL)
  - DROP TABLE ... PURGE is required for S3 Tables (managed Iceberg)
  - Namespaces are created via the Glue API (boto3), not Spark SQL
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

DB_BRONZE = args['database_name']
BUCKET = args['data_lake_bucket']
GOLD_TABLE_BUCKET_ARN = args['gold_table_bucket_arn']
AWS_REGION = args.get('aws_region', 'us-east-1')

# Parse the S3 Tables bucket ARN to extract account ID and bucket name
# ARN format: arn:aws:s3tables:region:ACCOUNT_ID:bucket/BUCKET_NAME
arn_parts = GOLD_TABLE_BUCKET_ARN.split(":")
ACCOUNT_ID = arn_parts[4]
TABLE_BUCKET_NAME = arn_parts[5].split("/")[1]

DB_SILVER = "ecommerce_silver"   # Namespace in S3 Tables bucket
CATALOG = "glue_catalog"         # Reads bronze from standard Glue Catalog
S3T_CAT = "s3tablescatalog"      # Writes silver to S3 Tables via federated catalog

sc = SparkContext()
glueContext = GlueContext(sc)

# TODO 1: Configure Spark with the S3 Tables catalog.
# Use SparkSession.builder to register a second catalog (s3tablescatalog) that
# routes to S3 Tables via the Glue Data Catalog federation.
#
# spark = SparkSession.builder \
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
#     .config(f"spark.sql.catalog.{S3T_CAT}", "org.apache.iceberg.spark.SparkCatalog") \
#     .config(f"spark.sql.catalog.{S3T_CAT}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
#     .config(f"spark.sql.catalog.{S3T_CAT}.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}") \
#     .config(f"spark.sql.catalog.{S3T_CAT}.warehouse", f"s3://{BUCKET}/silver-warehouse/") \
#     .getOrCreate()

spark = glueContext.spark_session  # Replace this with SparkSession.builder above

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info(f"S3 Tables catalog: {S3T_CAT} → {TABLE_BUCKET_NAME}")
logger.info(f"Reading bronze from: {CATALOG}.{DB_BRONZE}")

# TODO 2: Create the silver namespace in S3 Tables if it doesn't exist.
# Use the Glue API (boto3) — Spark SQL CREATE NAMESPACE doesn't work with S3 Tables.
#
# import boto3
# glue_client = boto3.client('glue', region_name=AWS_REGION)
# try:
#     glue_client.create_database(
#         CatalogId=f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}",
#         DatabaseInput={'Name': DB_SILVER}
#     )
# except Exception as e:
#     logger.info(f"Namespace already exists: {e}")


def create_silver_table(name, query):
    """Query bronze, write results to S3 Tables silver namespace."""
    fqn = f"{S3T_CAT}.{DB_SILVER}.{name}"

    # TODO 3: Execute the query, drop existing table with PURGE, write with writeTo.
    # S3 Tables requires PURGE when dropping managed Iceberg tables.
    #
    # df = spark.sql(query)
    # logger.info(f"{name}: {df.count()} rows to write")
    # spark.sql(f"DROP TABLE IF EXISTS {fqn} PURGE")
    # df.writeTo(fqn).using("iceberg").tableProperty("format-version", "2").create()
    pass


# ── 1. order_details ─────────────────────────────────────────────────────────
# TODO 4: Create silver.order_details by querying bronze.
# Join bronze.orders with bronze.users on user_id = id.
# Include:
#   - Order fields: order_id, user_id, total_amount, order_date (cast created_at to DATE)
#   - order_month: date_format(created_at, 'yyyy-MM')
#   - Customer fields: customer_name (concat first+last), email, city, state, registration_date
#   - Window functions:
#     * customer_order_sequence: COUNT(*) OVER (PARTITION BY user_id ORDER BY created_at ROWS UNBOUNDED PRECEDING)
#     * customer_lifetime_value: SUM(total_amount) OVER (same window)
#   - order_value_segment: CASE on total_amount (<50 Low, <200 Medium, <500 High, else Premium)
#   - customer_segment: CASE on COUNT(*) OVER (PARTITION BY user_id) (1=New, <=3 Repeat, <=10 Loyal, else VIP)
#   - Time features: order_day_of_week, order_hour
#   - silver_processed_at: current_timestamp()

# ── 2. enriched_events ───────────────────────────────────────────────────────
# TODO 5: Create silver.enriched_events by querying bronze.
# LEFT JOIN bronze.events with bronze.users (on user_id) and bronze.products (on product_id).
# Include:
#   - Event fields: event_id, session_id, page_url, source, event_type, event_timestamp
#   - User enrichment: user_name, user_email, days_since_registration, user_city, user_state
#   - Product enrichment: product_name, product_category, product_price
#   - revenue: product price when event_type = 'purchase', else NULL
#   - event_sequence_in_session: COUNT(*) OVER (PARTITION BY session_id ORDER BY event_timestamp)
#   - Time features: event_hour, event_day_of_week, event_date (cast to DATE)
# Filter: WHERE event_date >= DATE '2024-01-01'

# ── 3. product_performance ───────────────────────────────────────────────────
# TODO 6: Create silver.product_performance by querying bronze.
# LEFT JOIN bronze.products with bronze.events on product_id.
# Aggregate per product (GROUP BY product fields):
#   - Product fields: product_id, product_name, category, price, stock_quantity, is_active
#   - Event counts by type: page_views, add_to_carts, purchases, clicks, total_events
#   - Visitor metrics: unique_visitors (distinct user_id), unique_sessions
#   - Revenue: total_revenue (sum order_value for purchases), avg_purchase_value
#   - Conversion rates: view_to_purchase_rate, cart_to_purchase_rate

logger.info("Silver layer complete")
job.commit()
