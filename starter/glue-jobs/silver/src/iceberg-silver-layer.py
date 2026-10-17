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

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{S3T_CAT}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{S3T_CAT}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{S3T_CAT}.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}") \
    .config(f"spark.sql.catalog.{S3T_CAT}.warehouse", f"s3://{BUCKET}/silver-warehouse/") \
    .getOrCreate()

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
import boto3
glue_client = boto3.client('glue', region_name=AWS_REGION)
try:
    glue_client.create_database(
        CatalogId=f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}",
        DatabaseInput={'Name': DB_SILVER}
    )
except Exception as e:
    logger.info(f"Namespace already exists or is already available: {e}")


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
    df = spark.sql(query)
    logger.info(f"{name}: {df.count()} rows to write")
    spark.sql(f"DROP TABLE IF EXISTS {fqn} PURGE")
    df.writeTo(fqn).using("iceberg").tableProperty("format-version", "2").create()


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
create_silver_table("order_details", f"""
    SELECT o.order_id, o.user_id, o.total_amount, CAST(o.created_at AS DATE) AS order_date,
           date_format(o.created_at, 'yyyy-MM') AS order_month,
           concat_ws(' ', u.first_name, u.last_name) AS customer_name,
           u.email, u.city, u.state, CAST(u.created_at AS DATE) AS registration_date,
           COUNT(*) OVER (PARTITION BY o.user_id ORDER BY o.created_at
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS customer_order_sequence,
           SUM(o.total_amount) OVER (PARTITION BY o.user_id ORDER BY o.created_at
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS customer_lifetime_value,
           CASE WHEN o.total_amount < 50 THEN 'Low' WHEN o.total_amount < 200 THEN 'Medium'
                WHEN o.total_amount < 500 THEN 'High' ELSE 'Premium' END AS order_value_segment,
           CASE WHEN COUNT(*) OVER (PARTITION BY o.user_id) = 1 THEN 'New'
                WHEN COUNT(*) OVER (PARTITION BY o.user_id) <= 3 THEN 'Repeat'
                WHEN COUNT(*) OVER (PARTITION BY o.user_id) <= 10 THEN 'Loyal' ELSE 'VIP' END AS customer_segment,
           dayofweek(o.created_at) AS order_day_of_week, hour(o.created_at) AS order_hour,
           current_timestamp() AS silver_processed_at
    FROM {CATALOG}.{DB_BRONZE}.orders o
    JOIN {CATALOG}.{DB_BRONZE}.users u ON o.user_id = u.id
    WHERE COALESCE(o.is_deleted, false) = false
""")

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
create_silver_table("enriched_events", f"""
    SELECT e.event_id, e.user_id, e.session_id, e.page_url, e.source, e.event_type, e.event_timestamp,
           concat_ws(' ', u.first_name, u.last_name) AS user_name, u.email AS user_email,
           datediff(CAST(e.event_timestamp AS DATE), CAST(u.created_at AS DATE)) AS days_since_registration,
           u.city AS user_city, u.state AS user_state,
           p.name AS product_name, p.category AS product_category, p.price AS product_price,
           CASE WHEN e.event_type = 'purchase' THEN COALESCE(e.order_value, p.price) END AS revenue,
           COUNT(*) OVER (PARTITION BY e.session_id ORDER BY e.event_timestamp
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS event_sequence_in_session,
           hour(e.event_timestamp) AS event_hour, dayofweek(e.event_timestamp) AS event_day_of_week,
           CAST(e.event_timestamp AS DATE) AS event_date,
           current_timestamp() AS silver_processed_at
    FROM {CATALOG}.{DB_BRONZE}.events e
    LEFT JOIN {CATALOG}.{DB_BRONZE}.users u ON e.user_id = u.id
    LEFT JOIN {CATALOG}.{DB_BRONZE}.products p ON e.product_id = p.id
    WHERE CAST(e.event_timestamp AS DATE) >= DATE '2024-01-01'
""")

# ── 3. product_performance ───────────────────────────────────────────────────
# TODO 6: Create silver.product_performance by querying bronze.
# LEFT JOIN bronze.products with bronze.events on product_id.
# Aggregate per product (GROUP BY product fields):
#   - Product fields: product_id, product_name, category, price, stock_quantity, is_active
#   - Event counts by type: page_views, add_to_carts, purchases, clicks, total_events
#   - Visitor metrics: unique_visitors (distinct user_id), unique_sessions
#   - Revenue: total_revenue (sum order_value for purchases), avg_purchase_value
#   - Conversion rates: view_to_purchase_rate, cart_to_purchase_rate
create_silver_table("product_performance", f"""
    SELECT p.id AS product_id, p.name AS product_name, p.category, p.price,
           p.stock_quantity, p.is_active,
           SUM(CASE WHEN e.event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
           SUM(CASE WHEN e.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_carts,
           SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
           SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
           COUNT(e.event_id) AS total_events,
           COUNT(DISTINCT e.user_id) AS unique_visitors,
           COUNT(DISTINCT e.session_id) AS unique_sessions,
           SUM(CASE WHEN e.event_type = 'purchase' THEN COALESCE(e.order_value, 0.0) ELSE 0.0 END) AS total_revenue,
           AVG(CASE WHEN e.event_type = 'purchase' THEN e.order_value END) AS avg_purchase_value,
           CASE WHEN SUM(CASE WHEN e.event_type = 'page_view' THEN 1 ELSE 0 END) = 0 THEN 0.0
                ELSE SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 /
                     SUM(CASE WHEN e.event_type = 'page_view' THEN 1 ELSE 0 END) END AS view_to_purchase_rate,
           CASE WHEN SUM(CASE WHEN e.event_type = 'add_to_cart' THEN 1 ELSE 0 END) = 0 THEN 0.0
                ELSE SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 /
                     SUM(CASE WHEN e.event_type = 'add_to_cart' THEN 1 ELSE 0 END) END AS cart_to_purchase_rate,
           current_timestamp() AS silver_processed_at
    FROM {CATALOG}.{DB_BRONZE}.products p
    LEFT JOIN {CATALOG}.{DB_BRONZE}.events e ON p.id = e.product_id
    GROUP BY p.id, p.name, p.category, p.price, p.stock_quantity, p.is_active
""")

logger.info("Silver layer complete")
job.commit()
