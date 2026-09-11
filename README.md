# gcs-bigquery-airflow-etl

Public demo of a small production-style file pipeline:

**GCS landing file → validate → BigQuery `raw` (one business-date partition) → BigQuery `curated` (same date, light rules).**

This is a demo reference repo. The data is handwritten fake orders. It is not connected to any employer, or live warehouse.

## What you can reuse

- Business date is taken from the **filename / file payload / run conf**, not from the day Airflow starts.
- One run locks **one** business date.
- The same date can be rerun. Raw and curated for that date are replaced, not appended.
- A bad file fails validation and does not write a curated partition.

Optional later (not in this repo): a read-only page that answers “did date X arrive and pass?”.

## Layout

    dags/orders_daily_etl.py     Airflow DAG
    scripts/validate_orders.py   Same checks the DAG runs, usable locally
    scripts/run_once.py          Same load without Airflow (personal GCP smoke test)
    sql/                         raw + curated DDL and the curated rebuild
    sample/                      three good days + one bad file
    .env.example                 names only, no secrets

## Sample files

| File | Expected result |
| --- | --- |
| `sample/orders_2026-09-01.csv` | pass |
| `sample/orders_2026-09-02.csv` | pass |
| `sample/orders_2026-09-03.csv` | pass |
| `sample/orders_2026-09-04_bad.csv` | fail (bad timestamp, negative qty, date mismatch, missing `order_id`, duplicate id, short row) |

Filename contract: `orders_YYYY-MM-DD.csv`.

Columns: `order_id, order_ts, business_date, customer_id, sku, qty, unit_price, amount, currency, country, status`.

## Local check (no GCP)

    python scripts/validate_orders.py sample/orders_2026-09-01.csv
    python scripts/validate_orders.py sample/orders_2026-09-04_bad.csv

Good file prints `OK`. Bad file prints `FAIL` and the reasons. The locked date is read from the filename unless you pass `--business-date`.

## First run on a personal GCP project (no Airflow)

This is enough to prove the contract. Do not start with Cloud Composer.

1. Create a GCP project, link billing, enable **Cloud Storage** and **BigQuery**.
2. Create a bucket and leave BigQuery datasets to the script (`raw`, `curated`).
3. Copy `.env.example` to `.env`. Put in your project id and bucket name. Do not commit `.env`.
4. Login once:

        gcloud auth application-default login
        gcloud config set project YOUR_PROJECT_ID

5. Install the small client libs (not Airflow):

        pip install google-cloud-storage google-cloud-bigquery python-dotenv

6. Run a good day, then the same day again, then the bad file:

        python scripts/run_once.py sample/orders_2026-09-01.csv
        python scripts/run_once.py sample/orders_2026-09-01.csv
        python scripts/run_once.py sample/orders_2026-09-04_bad.csv

Good day prints `OK raw=5 curated=5`. The second run must print the same counts. The bad file must print `FAIL` and must not change BigQuery.

## What the DAG does

Manual trigger only (`schedule=None`). You must pass the date:

    { "business_date": "2026-09-01" }

Then:

1. Lock that date. Refuse a run with no date. Do not default to today.
2. Require `gs://$GCS_BUCKET/landing/orders/orders_2026-09-01.csv`.
3. Download, validate, copy the accepted file to `landing/orders/_accepted/`.
4. Create `raw.orders` and `curated.orders` if they are missing.
5. Replace the raw partition for that date (delete date + insert).
6. Rebuild the curated partition for that date only.

Curated rules in this demo are intentionally thin: normalize case, keep `qty > 0`, flag whether `amount` matches `qty * unit_price`.

## Wire-up sketch

1. Create a GCP project, a bucket, and BigQuery datasets `raw` and `curated` (or change the names in `.env`).
2. Copy `.env.example` to `.env` on the Airflow box. Point `GOOGLE_APPLICATION_CREDENTIALS` at a key that can read the bucket and write those datasets, or use the platform’s default credentials.
3. Drop this repo so Airflow can import `dags/orders_daily_etl.py` and can read `scripts/` + `sql/` (sibling folders).
4. Upload a good sample file:

        gsutil cp sample/orders_2026-09-01.csv gs://$GCS_BUCKET/landing/orders/orders_2026-09-01.csv

5. Trigger `orders_daily_etl` with `business_date=2026-09-01`.
6. Trigger again with the same date to show the partition replace.
7. Trigger `2026-09-04` against the bad file and confirm the run stops before curated.

Exact Composer / Cloud Composer / MWAA install steps stay out of this repo on purpose. The DAG is the contract.

## Out of scope

- UI / ops page
- Multi-source fan-in
- SCD2, late-arriving dimensions, CDC
- Real credentials, real tables, real client files
- Anything that looks like a copy of an employer warehouse

## Positioning

Banking-style production ETL in public form: land the file, prove the business date, write a replaceable daily partition, keep curated boring.

If you want this pattern stood up on your GCP project, the setup is one source, one landing prefix, two datasets, and a rerunnable DAG.