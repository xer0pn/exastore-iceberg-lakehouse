"""
AWS Glue Job: Events Bronze Layer
Reads clickstream events JSON from S3, validates data quality,
writes to Iceberg table via SparkSQL.

Your task: Complete the TODOs below to build a working events pipeline.
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'database_name', 'data_lake_bucket',
    'events_s3_path', 'batch_size', 'aws_region'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DATABASE = args['database_name']
BUCKET = args['data_lake_bucket']
EVENTS_PATH = args['events_s3_path']
CATALOG = "glue_catalog"

FQN = f"{CATALOG}.{DATABASE}.events"
LOCATION = f"s3://{BUCKET}/warehouse/{DATABASE}/events"

# ── TODO 1: Create the events Iceberg table ──────────────────────────────────
# Write a CREATE TABLE IF NOT EXISTS statement for the events table.
# Schema (all nullable):
#   event_id (STRING), event_type (STRING), user_id (BIGINT), product_id (BIGINT),
#   event_timestamp (TIMESTAMP), session_id (STRING), page_url (STRING),
#   source (STRING), quantity (BIGINT), cart_value (DOUBLE),
#   click_position (STRING), click_target (STRING), currency (STRING),
#   order_id (BIGINT), order_value (DOUBLE), payment_method (STRING),
#   referrer (STRING), processing_time (TIMESTAMP),
#   year (INT), month (INT), day (INT)
#
# Partition by: (year, month)
# Use TBLPROPERTIES for format-version=2, parquet, snappy compression.

logger.info(f"Table ready: {FQN}")

# ── Read events JSON ─────────────────────────────────────────────────────────

raw = spark.read.option("multiline", "true").json(EVENTS_PATH)
raw_count = raw.count()
logger.info(f"Read {raw_count} raw event records")

if raw_count == 0:
    logger.warning("No events found, exiting")
    job.commit()
    sys.exit(0)

# ── TODO 2: Transform raw events ─────────────────────────────────────────────
# Create a temp view "raw_events" from the raw DataFrame, then write a SQL
# query to create "parsed_events" that:
#   - Casts user_id, product_id, quantity, order_id to BIGINT
#   - Casts cart_value, order_value to DOUBLE
#   - Parses event_timestamp with TO_TIMESTAMP()
#   - Adds processing_time = current_timestamp()
#   - Extracts year, month, day from event_timestamp
#   - Filters out records where event_id IS NULL or event_timestamp IS NULL

# ── TODO 3: Data quality checks ──────────────────────────────────────────────
# Write a single-pass aggregation query on parsed_events to count:
#   - total records
#   - records with invalid event_type (not in: page_view, add_to_cart, purchase, click)
#   - records with future timestamps (> current_timestamp())
#   - records with null user_id
# Log the results. These metrics help monitor data quality over time.

# ── TODO 4: Filter and insert ─────────────────────────────────────────────────
# Create a "valid_events" view that filters parsed_events to only include:
#   - Valid event types
#   - Non-future timestamps
#   - Non-null user_id with user_id >= 1
# Then INSERT INTO the Iceberg table from valid_events.

# ── Log results ───────────────────────────────────────────────────────────────

final_count = spark.sql(f"SELECT COUNT(*) AS c FROM {FQN}").collect()[0]['c']
logger.info(f"Events table now has {final_count} total rows")

# TODO 5 (optional): Log event type breakdown from the final table

logger.info("Events bronze layer complete")
job.commit()
