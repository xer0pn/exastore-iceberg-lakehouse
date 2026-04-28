# Apache Iceberg Lakehouse on AWS

Build a medallion-architecture data lakehouse using Apache Iceberg, AWS Glue, S3 Tables, and Athena. Ingest CDC and clickstream data through Bronze → Silver → Gold layers, then explore Iceberg features like snapshots and time-travel.

```
CDC JSON (S3)    ──→  Glue: CDC Bronze    ──→  S3 Iceberg (orders, users, products)
Events JSON (S3) ──→  Glue: Events Bronze ──→  S3 Iceberg (events)
                      Glue: Silver Layer   ──→  S3 Tables  (order_details, enriched_events, product_performance)
                      Glue: Gold Layer     ──→  S3 Tables  (customer_analytics, realtime_metrics)
```

## What You'll Build

| Layer | Storage | Tables | Technique |
|-------|---------|--------|-----------|
| Bronze | S3 + Glue Catalog Iceberg | orders, users, products, events | CDC MERGE INTO, append-only INSERT |
| Silver | S3 Tables (managed Iceberg) | order_details, enriched_events, product_performance | JOINs, window functions, aggregations |
| Gold | S3 Tables (managed Iceberg) | customer_analytics, realtime_metrics | Monthly/hourly business aggregations |

## Repo Structure

```
starter-code/           ← Your working directory
├── project.ipynb       ← Main notebook: deploy, run jobs, validate, explore Iceberg
├── glue-jobs/          ← 4 Glue ETL scripts with TODOs to complete
│   ├── cdc-batch/      ← CDC Bronze (MERGE INTO)
│   ├── events/         ← Events Bronze (INSERT)
│   ├── silver/         ← Silver enrichment layer
│   └── gold/           ← Gold aggregation layer
└── datasets/           ← CDC and clickstream JSON data

project-solution/       ← Reference solution (don't peek until you're stuck)
```

## Getting Started

1. Deploy the CloudFormation stack (provided in the lab environment)
2. Open `starter-code/project.ipynb`
3. Fill in your resource names from the stack outputs
4. Complete the TODOs in the 4 Glue job scripts
5. Run the notebook end-to-end to validate

## AWS Services Used

- **AWS Glue** — Spark ETL jobs for each medallion layer
- **Amazon S3** — Raw data and Bronze Iceberg tables
- **S3 Tables** — Managed Iceberg for Silver and Gold layers
- **Amazon Athena** — SQL validation queries
- **Apache Iceberg** — Table format with MERGE INTO, snapshots, and time-travel
