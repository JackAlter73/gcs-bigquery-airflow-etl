#!/usr/bin/env python3
"""Run one business date without Airflow.

Same contract as the DAG: lock a date, validate, replace raw, rebuild curated.
Use this to prove the pipeline on a personal GCP project.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

from validate_orders import parse_business_date, validate  # noqa: E402


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise SystemExit(f"Missing {name}. Copy .env.example to .env and fill it in.")
    return value


def render_sql(name: str, business_date: str) -> str:
    text = (REPO_ROOT / "sql" / name).read_text(encoding="utf-8")
    return (
        text.replace("{{project}}", env("GCP_PROJECT_ID"))
        .replace("{{location}}", env("GCP_LOCATION", "US"))
        .replace("{{dataset_raw}}", env("BQ_DATASET_RAW", "raw"))
        .replace("{{dataset_curated}}", env("BQ_DATASET_CURATED", "curated"))
        .replace("{{table}}", env("BQ_TABLE_ORDERS", "orders"))
        .replace("{{business_date}}", business_date)
    )


def run_sql(client, sql: str) -> None:
    client.query(sql).result()


def main() -> int:
    parser = argparse.ArgumentParser(description="Lock one business date and load it.")
    parser.add_argument("csv_path", type=Path)
    parser.add_argument(
        "--business-date",
        help="YYYY-MM-DD. Defaults to the date in the filename.",
    )
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")

    csv_path: Path = args.csv_path
    if not csv_path.exists():
        print(f"ERROR file not found: {csv_path}", file=sys.stderr)
        return 2

    if args.business_date:
        business_date = parse_business_date(args.business_date).isoformat()
    else:
        from validate_orders import business_date_from_name

        inferred = business_date_from_name(csv_path)
        if inferred is None:
            print("ERROR pass --business-date or use orders_YYYY-MM-DD.csv", file=sys.stderr)
            return 2
        business_date = inferred.isoformat()

    errors = validate(csv_path, parse_business_date(business_date))
    if errors:
        print(f"FAIL {csv_path.name} locked_date={business_date}")
        for item in errors:
            print(f"  - {item}")
        return 1

    from google.cloud import bigquery, storage
    from google.cloud.bigquery import LoadJobConfig, SourceFormat, WriteDisposition

    project = env("GCP_PROJECT_ID")
    bucket_name = env("GCS_BUCKET")
    prefix = env("GCS_LANDING_PREFIX", "landing/orders")
    location = env("GCP_LOCATION", "US")
    raw_dataset = env("BQ_DATASET_RAW", "raw")
    orders_table = env("BQ_TABLE_ORDERS", "orders")

    landing_object = f"{prefix}/orders_{business_date}.csv"
    accepted_object = f"{prefix}/_accepted/orders_{business_date}.csv"

    storage_client = storage.Client(project=project)
    bucket = storage_client.bucket(bucket_name)
    bucket.blob(landing_object).upload_from_filename(str(csv_path))
    bucket.blob(accepted_object).upload_from_filename(str(csv_path))
    print(f"OK uploaded gs://{bucket_name}/{landing_object}")

    bq = bigquery.Client(project=project, location=location)
    run_sql(bq, render_sql("create_raw_orders.sql", business_date))
    run_sql(bq, render_sql("create_curated_orders.sql", business_date))

    staging = f"{project}.{raw_dataset}._stg_orders_{business_date.replace('-', '')}"
    raw_table = f"{project}.{raw_dataset}.{orders_table}"
    uri = f"gs://{bucket_name}/{accepted_object}"
    job_config = LoadJobConfig(
        source_format=SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
    )
    bq.load_table_from_uri(uri, staging, job_config=job_config).result()
    run_sql(
        bq,
        f"""
        DELETE FROM `{raw_table}`
        WHERE business_date = DATE "{business_date}";

        INSERT INTO `{raw_table}` (
          order_id, order_ts, business_date, customer_id, sku,
          qty, unit_price, amount, currency, country, status,
          source_file, ingested_at
        )
        SELECT
          order_id,
          order_ts,
          DATE(business_date),
          customer_id,
          sku,
          qty,
          unit_price,
          amount,
          currency,
          country,
          status,
          "{accepted_object}",
          CURRENT_TIMESTAMP()
        FROM `{staging}`;

        DROP TABLE `{staging}`;
        """,
    )
    run_sql(bq, render_sql("rebuild_curated_orders.sql", business_date))

    raw_count = list(
        bq.query(
            f"SELECT COUNT(*) AS n FROM `{raw_table}` WHERE business_date = DATE '{business_date}'"
        ).result()
    )[0]["n"]
    curated_table = (
        f"{project}.{env('BQ_DATASET_CURATED', 'curated')}.{orders_table}"
    )
    curated_count = list(
        bq.query(
            f"SELECT COUNT(*) AS n FROM `{curated_table}` WHERE business_date = DATE '{business_date}'"
        ).result()
    )[0]["n"]

    print(f"OK raw={raw_count} curated={curated_count} locked_date={business_date}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())