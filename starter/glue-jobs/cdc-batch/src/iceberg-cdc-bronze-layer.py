"""
AWS Glue Job: CDC Bronze Layer
Reads CDC JSON from S3, creates Iceberg tables, applies MERGE INTO for upserts/deletes.

Your task: Complete the TODOs below to build a working CDC pipeline.
The Glue job shell is pre-configured with Iceberg dependencies and Spark catalog settings.
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
    'cdc_data_path', 'target_year', 'aws_region'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DATABASE = args['database_name']
BUCKET = args['data_lake_bucket']
CDC_PATH = args['cdc_data_path']
TARGET_YEAR = args.get('target_year', '2025')
CATALOG = "glue_catalog"

# ── Table definitions ────────────────────────────────────────────────────────
# Define the schema, primary key, and partitioning for each CDC table.
#
# CDC records have this structure:
#   { "before": {...}, "after": {...}, "op": "c|u|d", "ts_ms": ..., "source": {"lsn": ...} }
#
# For creates/updates: business data is in "after"
# For deletes: business data is in "before"

TABLES = {
    "orders": {
        "pk": "order_id",
        "partition": "days(created_at)",
        # TODO 1: Define the column list for the orders Iceberg table.
        # Columns from CDC: order_id (BIGINT), user_id (BIGINT), order_date (TIMESTAMP),
        #   status (STRING), total_amount (DECIMAL(10,2)), payment_method (STRING),
        #   created_at (TIMESTAMP), updated_at (TIMESTAMP), is_deleted (BOOLEAN)
        # CDC metadata: op (STRING), ts_ms (BIGINT), source_lsn (BIGINT), _processed_at (TIMESTAMP)
        "columns": """
        """,
        # TODO 2: Write the SELECT expression to extract fields from the CDC JSON.
        # Use COALESCE(after.order_id, before.order_id) for the primary key (handles deletes).
        # Cast timestamps with CAST(after.order_date AS TIMESTAMP).
        # Cast decimals with CAST(after.total_amount AS DECIMAL(10,2)).
        # Get source_lsn from source.lsn
        # Add current_timestamp() AS _processed_at
        "extract": """
        """,
    },

    # TODO 3: Define the "users" table entry following the same pattern as orders.
    # Primary key: id. Partition: months(created_at).
    # Columns: id, first_name, last_name, email, age, gender, state, city, country,
    #          created_at, updated_at, is_deleted, op, ts_ms, source_lsn, _processed_at

    # TODO 4: Define the "products" table entry.
    # Primary key: id. Partition: months(created_at).
    # Columns: id, name, description, category, price (DECIMAL(10,2)), sku,
    #          stock_quantity, is_active (BOOLEAN), created_at, updated_at, is_deleted,
    #          op, ts_ms, source_lsn, _processed_at
}


def create_table(table_name, cfg):
    """Create an Iceberg table if it doesn't exist."""
    fqn = f"{CATALOG}.{DATABASE}.{table_name}"
    location = f"s3://{BUCKET}/warehouse/{DATABASE}/{table_name}"

    # TODO 5: Write the CREATE TABLE IF NOT EXISTS statement.
    # Use: USING iceberg, PARTITIONED BY (cfg['partition']),
    #      LOCATION, and TBLPROPERTIES for format-version, parquet, snappy.
    pass

    logger.info(f"Table ready: {fqn}")


def process_table(table_name, cfg):
    """Read CDC JSON, deduplicate, and MERGE INTO the Iceberg table."""
    pk = cfg["pk"]
    fqn = f"{CATALOG}.{DATABASE}.{table_name}"
    s3_path = f"{CDC_PATH}/year={TARGET_YEAR}/table_name={table_name}/"

    # Read CDC JSON files
    raw = spark.read.option("multiline", "true").json(s3_path)
    raw_count = raw.count()
    logger.info(f"{table_name}: {raw_count} raw CDC records")

    if raw_count == 0:
        return True

    raw.createOrReplaceTempView(f"raw_{table_name}")

    # TODO 6: Create a temp view "cdc_{table_name}" that extracts business fields
    # using cfg['extract']. Filter to only valid ops: WHERE op IN ('c', 'u', 'd')

    # TODO 7: Create a temp view "staged_{table_name}" that deduplicates records.
    # Keep only the latest record per primary key using ROW_NUMBER() with
    # ORDER BY source_lsn DESC, ts_ms DESC. Filter WHERE _rn = 1.
    # Also filter out NULL primary keys.

    staged_count = spark.sql(f"SELECT COUNT(*) AS c FROM staged_{table_name}").collect()[0]['c']
    logger.info(f"{table_name}: {staged_count} records after dedup")

    # Log operation breakdown
    ops = spark.sql(f"SELECT op, COUNT(*) AS cnt FROM staged_{table_name} GROUP BY op").collect()
    for row in ops:
        op_label = {'c': 'INSERT', 'u': 'UPDATE', 'd': 'DELETE'}.get(row['op'], row['op'])
        logger.info(f"  {op_label}: {row['cnt']}")

    # TODO 8: Write the MERGE INTO statement.
    # First, create a clean source view without the _rn column.
    # Note: SELECT * EXCEPT is not supported in Spark SQL on Glue 5.0.
    # Instead, build the column list dynamically:
    #   cols = [c for c in spark.sql(f"SELECT * FROM staged_{table_name}").columns if c != '_rn']
    #   col_list = ', '.join(cols)
    # Then create a view with SELECT {col_list} FROM staged_{table_name}
    #
    # Then MERGE INTO {fqn} t USING source s ON t.{pk} = s.{pk}
    #   WHEN MATCHED AND s.op = 'd' THEN DELETE
    #   WHEN MATCHED THEN UPDATE SET *
    #   WHEN NOT MATCHED AND s.op != 'd' THEN INSERT *

    final_count = spark.sql(f"SELECT COUNT(*) AS c FROM {fqn}").collect()[0]['c']
    logger.info(f"{table_name}: MERGE complete — {final_count} rows in table")
    return True


def main():
    logger.info(f"CDC Bronze Layer — DB: {DATABASE}, Year: {TARGET_YEAR}")

    results = {}
    for table_name, cfg in TABLES.items():
        try:
            create_table(table_name, cfg)
            results[table_name] = process_table(table_name, cfg)
        except Exception as e:
            logger.error(f"{table_name} failed: {e}")
            results[table_name] = False

    succeeded = sum(1 for v in results.values() if v)
    logger.info(f"Summary: {succeeded}/{len(TABLES)} tables processed")
    job.commit()


if __name__ == "__main__":
    main()
