"""Orders ETL: GCS landing file -> BigQuery raw partition -> curated partition.

A run locks one business_date. That date comes from dag_run.conf / params.
It is not "whatever day the DAG happens to start".

Rerun the same business_date by triggering again with the same conf.
Raw and curated partitions are replaced, not appended.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

LOGGER = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

from validate_orders import (  # noqa: E402
    business_date_from_name,
    parse_business_date,
    validate,
)


GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
GCP_LOCATION = os.getenv("GCP_LOCATION", "US")
GCS_BUCKET = os.getenv("GCS_BUCKET", "your-demo-bucket")
GCS_LANDING_PREFIX = os.getenv("GCS_LANDING_PREFIX", "landing/orders")
BQ_DATASET_RAW = os.getenv("BQ_DATASET_RAW", "raw")
BQ_DATASET_CURATED = os.getenv("BQ_DATASET_CURATED", "curated")
BQ_TABLE_ORDERS = os.getenv("BQ_TABLE_ORDERS", "orders")


def _object_name(business_date: str) -> str:
    return f"{GCS_LANDING_PREFIX}/orders_{business_date}.csv"


def _render_sql(name: str, business_date: str) -> str:
    text = (REPO_ROOT / "sql" / name).read_text(encoding="utf-8")
    return (
        text.replace("{{project}}", GCP_PROJECT_ID)
        .replace("{{location}}", GCP_LOCATION)
        .replace("{{dataset_raw}}", BQ_DATASET_RAW)
        .replace("{{dataset_curated}}", BQ_DATASET_CURATED)
        .replace("{{table}}", BQ_TABLE_ORDERS)
        .replace("{{business_date}}", business_date)
    )


def _bq_client():
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    hook = BigQueryHook(location=GCP_LOCATION, use_legacy_sql=False)
    return hook.get_client(project_id=GCP_PROJECT_ID, location=GCP_LOCATION)


def _run_sql(sql: str) -> None:
    job = _bq_client().query(sql)
    job.result()


with DAG(
    dag_id="orders_daily_etl",
    description="Lock one business date, land a CSV into raw, rebuild curated.",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "contractor",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "business_date": Param(
            "",
            type=["null", "string"],
            description="YYYY-MM-DD. Required. Must match the landing filename.",
        )
    },
    tags=["orders", "gcs", "bigquery", "demo"],
    doc_md=__doc__,
) as dag:

    @task
    def resolve_business_date(**context) -> str:
        conf = context["dag_run"].conf or {}
        param_value = context["params"].get("business_date") or ""
        raw_value = (conf.get("business_date") or param_value or "").strip()
        if not raw_value:
            raise ValueError(
                "business_date is required. Trigger with "
                '{"business_date": "2026-09-01"}. Do not use the clock date.'
            )
        locked = parse_business_date(raw_value)
        LOGGER.info("Locked business_date=%s", locked.isoformat())
        return locked.isoformat()

    @task
    def assert_landing_file(business_date: str) -> str:
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        object_name = _object_name(business_date)
        hook = GCSHook()
        if not hook.exists(bucket_name=GCS_BUCKET, object_name=object_name):
            raise FileNotFoundError(
                f"gs://{GCS_BUCKET}/{object_name} is not present for {business_date}"
            )
        inferred = business_date_from_name(Path(object_name))
        if inferred is None or inferred.isoformat() != business_date:
            raise ValueError(
                f"object name {object_name} does not lock business_date {business_date}"
            )
        return object_name

    @task
    def validate_and_stage(business_date: str, object_name: str) -> str:
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        hook = GCSHook()
        with tempfile.TemporaryDirectory() as tmp:
            local_path = Path(tmp) / Path(object_name).name
            hook.download(
                bucket_name=GCS_BUCKET,
                object_name=object_name,
                filename=str(local_path),
            )
            errors = validate(local_path, parse_business_date(business_date))
            if errors:
                detail = "; ".join(errors[:12])
                raise ValueError(
                    f"{object_name} failed validation for {business_date}: {detail}"
                )
            staged = f"{GCS_LANDING_PREFIX}/_accepted/orders_{business_date}.csv"
            hook.upload(
                bucket_name=GCS_BUCKET,
                object_name=staged,
                filename=str(local_path),
            )
        LOGGER.info("Accepted %s -> gs://%s/%s", object_name, GCS_BUCKET, staged)
        return staged

    @task
    def ensure_tables() -> str:
        _run_sql(_render_sql("create_raw_orders.sql", "2000-01-01"))
        _run_sql(_render_sql("create_curated_orders.sql", "2000-01-01"))
        return "tables-ready"

    @task
    def load_raw_partition(business_date: str, staged_object: str) -> str:
        from google.cloud.bigquery import LoadJobConfig, SourceFormat, WriteDisposition

        client = _bq_client()
        staging_table = (
            f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}._stg_orders_{business_date.replace('-', '')}"
        )
        job_config = LoadJobConfig(
            source_format=SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            allow_quoted_newlines=True,
        )
        uri = f"gs://{GCS_BUCKET}/{staged_object}"
        client.load_table_from_uri(uri, staging_table, job_config=job_config).result()

        raw_table = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.{BQ_TABLE_ORDERS}"
        client.query(
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
              "{staged_object}",
              CURRENT_TIMESTAMP()
            FROM `{staging_table}`;

            DROP TABLE `{staging_table}`;
            """
        ).result()
        LOGGER.info("Replaced raw partition business_date=%s from %s", business_date, uri)
        return f"{raw_table}@{business_date}"

    @task
    def rebuild_curated_partition(business_date: str) -> str:
        _run_sql(_render_sql("rebuild_curated_orders.sql", business_date))
        return f"{GCP_PROJECT_ID}.{BQ_DATASET_CURATED}.{BQ_TABLE_ORDERS}@{business_date}"

    locked_date = resolve_business_date()
    landing = assert_landing_file(locked_date)
    staged = validate_and_stage(locked_date, landing)
    prepared = ensure_tables()
    raw_table = load_raw_partition(locked_date, staged)
    curated = rebuild_curated_partition(locked_date)

    prepared >> raw_table >> curated
