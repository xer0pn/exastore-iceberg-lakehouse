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
            order_id BIGINT,
            user_id BIGINT,
            order_date TIMESTAMP,
            status STRING,
            total_amount DECIMAL(10,2),
            payment_method STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            is_deleted BOOLEAN,
            op STRING,
            ts_ms BIGINT,
            source_lsn BIGINT,
            _processed_at TIMESTAMP
        """,
        # TODO 2: Write the SELECT expression to extract fields from the CDC JSON.
        # Use COALESCE(after.order_id, before.order_id) for the primary key (handles deletes).
        # Cast timestamps with CAST(after.order_date AS TIMESTAMP).
        # Cast decimals with CAST(after.total_amount AS DECIMAL(10,2)).
        # Get source_lsn from source.lsn
        # Add current_timestamp() AS _processed_at
        "extract": """
            COALESCE(after.order_id, before.order_id) AS order_id,
            COALESCE(after.user_id, before.user_id) AS user_id,
            CAST(COALESCE(after.order_date, before.order_date) AS TIMESTAMP) AS order_date,
            COALESCE(after.status, before.status) AS status,
            CAST(COALESCE(after.total_amount, before.total_amount) AS DECIMAL(10,2)) AS total_amount,
            COALESCE(after.payment_method, before.payment_method) AS payment_method,
            CAST(COALESCE(after.created_at, before.created_at) AS TIMESTAMP) AS created_at,
            CAST(COALESCE(after.updated_at, before.updated_at) AS TIMESTAMP) AS updated_at,
            COALESCE(after.is_deleted, before.is_deleted) AS is_deleted,
            op,
            CAST(ts_ms AS BIGINT) AS ts_ms,
            CAST(source.lsn AS BIGINT) AS source_lsn,
            current_timestamp() AS _processed_at
        """,
    },

    # TODO 3: Define the "users" table entry following the same pattern as orders.
    # Primary key: id. Partition: months(created_at).
    # Columns: id, first_name, last_name, email, age, gender, state, city, country,
    #          created_at, updated_at, is_deleted, op, ts_ms, source_lsn, _processed_at

    "users": {
        "pk": "id",
        "partition": "months(created_at)",
        "columns": """
            id BIGINT, first_name STRING, last_name STRING, email STRING,
            age BIGINT, gender STRING, state STRING, city STRING, country STRING,
            created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted BOOLEAN,
            op STRING, ts_ms BIGINT, source_lsn BIGINT, _processed_at TIMESTAMP
        """,
        "extract": """
            COALESCE(after.id, before.id) AS id,
            COALESCE(after.first_name, before.first_name) AS first_name,
            COALESCE(after.last_name, before.last_name) AS last_name,
            COALESCE(after.email, before.email) AS email,
            COALESCE(after.age, before.age) AS age,
            COALESCE(after.gender, before.gender) AS gender,
            COALESCE(after.state, before.state) AS state,
            COALESCE(after.city, before.city) AS city,
            COALESCE(after.country, before.country) AS country,
            CAST(COALESCE(after.created_at, before.created_at) AS TIMESTAMP) AS created_at,
            CAST(COALESCE(after.updated_at, before.updated_at) AS TIMESTAMP) AS updated_at,
            COALESCE(after.is_deleted, before.is_deleted) AS is_deleted,
            op, CAST(ts_ms AS BIGINT) AS ts_ms, CAST(source.lsn AS BIGINT) AS source_lsn,
            current_timestamp() AS _processed_at
        """,
    },

    # TODO 4: Define the "products" table entry.
    # Primary key: id. Partition: months(created_at).
    # Columns: id, name, description, category, price (DECIMAL(10,2)), sku,
    #          stock_quantity, is_active (BOOLEAN), created_at, updated_at, is_deleted,
    #          op, ts_ms, source_lsn, _processed_at
    "products": {
        "pk": "id",
        "partition": "months(created_at)",
        "columns": """
            id BIGINT, name STRING, description STRING, category STRING,
            price DECIMAL(10,2), sku STRING, stock_quantity BIGINT,
            is_active BOOLEAN, created_at TIMESTAMP, updated_at TIMESTAMP,
            is_deleted BOOLEAN, op STRING, ts_ms BIGINT, source_lsn BIGINT,
            _processed_at TIMESTAMP
        """,
        "extract": """
            COALESCE(after.id, before.id) AS id,
            COALESCE(after.name, before.name) AS name,
            COALESCE(after.description, before.description) AS description,
            COALESCE(after.category, before.category) AS category,
            CAST(COALESCE(after.price, before.price) AS DECIMAL(10,2)) AS price,
            COALESCE(after.sku, before.sku) AS sku,
            COALESCE(after.stock_quantity, before.stock_quantity) AS stock_quantity,
            COALESCE(after.is_active, before.is_active) AS is_active,
            CAST(COALESCE(after.created_at, before.created_at) AS TIMESTAMP) AS created_at,
            CAST(COALESCE(after.updated_at, before.updated_at) AS TIMESTAMP) AS updated_at,
            COALESCE(after.is_deleted, before.is_deleted) AS is_deleted,
            op, CAST(ts_ms AS BIGINT) AS ts_ms, CAST(source.lsn AS BIGINT) AS source_lsn,
            current_timestamp() AS _processed_at
        """,
    },
}


def create_table(table_name, cfg):
    """Create an Iceberg table if it doesn't exist."""
    fqn = f"{CATALOG}.{DATABASE}.{table_name}"
    location = f"s3://{BUCKET}/warehouse/{DATABASE}/{table_name}"

    # TODO 5: Write the CREATE TABLE IF NOT EXISTS statement.
    # Use: USING iceberg, PARTITIONED BY (cfg['partition']),
    #      LOCATION, and TBLPROPERTIES for format-version, parquet, snappy.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            {cfg['columns']}
        ) USING iceberg
        PARTITIONED BY ({cfg['partition']})
        LOCATION '{location}'
        TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet',
                       'write.parquet.compression-codec'='snappy')
    """)

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

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW cdc_{table_name} AS
        SELECT {cfg['extract']}
        FROM raw_{table_name}
        WHERE op IN ('c', 'u', 'd')
    """)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW staged_{table_name} AS
        SELECT * FROM (
            SELECT c.*, ROW_NUMBER() OVER (
                PARTITION BY {pk} ORDER BY source_lsn DESC, ts_ms DESC
            ) AS _rn
            FROM cdc_{table_name} c
            WHERE {pk} IS NOT NULL
        ) ranked
        WHERE _rn = 1
    """)

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

    cols = [c for c in spark.sql(f"SELECT * FROM staged_{table_name}").columns if c != '_rn']
    col_list = ', '.join(cols)
    spark.sql(f"CREATE OR REPLACE TEMP VIEW source_{table_name} AS SELECT {col_list} FROM staged_{table_name}")
    spark.sql(f"""
        MERGE INTO {fqn} t
        USING source_{table_name} s
        ON t.{pk} = s.{pk}
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT *
    """)

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
