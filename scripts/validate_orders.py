#!/usr/bin/env python3
"""Validate an orders CSV against the demo contract.

Business date comes from the filename (orders_YYYY-MM-DD.csv) or --business-date.
It is not taken from the calendar day the script runs.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime
from pathlib import Path

REQUIRED_COLUMNS = [
    "order_id",
    "order_ts",
    "business_date",
    "customer_id",
    "sku",
    "qty",
    "unit_price",
    "amount",
    "currency",
    "country",
    "status",
]

FILENAME_RE = re.compile(r"orders_(\d{4}-\d{2}-\d{2})")


def parse_business_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def business_date_from_name(path: Path) -> date | None:
    match = FILENAME_RE.search(path.name)
    if not match:
        return None
    return parse_business_date(match.group(1))


def load_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    import csv

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [{k: (v or "").strip() for k, v in row.items()} for row in reader]
    return fieldnames, rows


def validate(path: Path, business_date: date) -> list[str]:
    errors: list[str] = []
    fieldnames, rows = load_rows(path)

    missing = [col for col in REQUIRED_COLUMNS if col not in fieldnames]
    if missing:
        errors.append(f"missing columns: {', '.join(missing)}")
        return errors

    if not rows:
        errors.append("file has a header but no data rows")
        return errors

    seen_ids: dict[str, int] = {}
    for idx, row in enumerate(rows, start=2):
        prefix = f"row {idx}"
        order_id = row.get("order_id", "")
        if not order_id:
            errors.append(f"{prefix}: empty order_id")
        elif order_id in seen_ids:
            errors.append(f"{prefix}: duplicate order_id {order_id} (also row {seen_ids[order_id]})")
        else:
            seen_ids[order_id] = idx

        try:
            row_date = parse_business_date(row.get("business_date", ""))
            if row_date != business_date:
                errors.append(
                    f"{prefix}: business_date {row_date} does not match locked date {business_date}"
                )
        except ValueError:
            errors.append(f"{prefix}: invalid business_date {row.get('business_date')!r}")

        try:
            datetime.fromisoformat(row.get("order_ts", "").replace("Z", "+00:00"))
        except ValueError:
            errors.append(f"{prefix}: invalid order_ts {row.get('order_ts')!r}")

        try:
            qty = int(row.get("qty", ""))
            if qty <= 0:
                errors.append(f"{prefix}: qty must be > 0, got {qty}")
        except ValueError:
            errors.append(f"{prefix}: qty is not an integer")

        for numeric_col in ("unit_price", "amount"):
            try:
                float(row.get(numeric_col, ""))
            except ValueError:
                errors.append(f"{prefix}: {numeric_col} is not numeric")

        if not row.get("status"):
            errors.append(f"{prefix}: empty status")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a demo orders CSV.")
    parser.add_argument("csv_path", type=Path)
    parser.add_argument(
        "--business-date",
        help="Lock this business date. Defaults to the date in the filename.",
    )
    args = parser.parse_args()

    path: Path = args.csv_path
    if not path.exists():
        print(f"ERROR file not found: {path}", file=sys.stderr)
        return 2

    try:
        if args.business_date:
            business_date = parse_business_date(args.business_date)
        else:
            inferred = business_date_from_name(path)
            if inferred is None:
                print(
                    "ERROR cannot infer business date from filename; pass --business-date",
                    file=sys.stderr,
                )
                return 2
            business_date = inferred
    except ValueError as exc:
        print(f"ERROR invalid business date: {exc}", file=sys.stderr)
        return 2

    _, rows = load_rows(path)
    errors = validate(path, business_date)
    if errors:
        print(
            f"FAIL {path.name} locked_date={business_date} "
            f"rows={len(rows)} errors={len(errors)}"
        )
        for item in errors:
            print(f"  - {item}")
        return 1
    print(f"OK {path.name} locked_date={business_date} rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
