"""Idempotent loader for the second sales feed (provisional CSV drop).

Lands accepted rows in new_source.sales_landing keyed by (source_key,
source_version) and quarantines bad rows in new_source.sales_rejects. Replaying
a batch is a no-op. Unlike the legacy loader, failures are raised so that the
Airflow task fails. The legacy loader is not touched by this module.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json, execute_values

import new_source_contract as contract

SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS new_source;
CREATE TABLE IF NOT EXISTS new_source.load_batches (
    load_batch_id   TEXT PRIMARY KEY,
    source_file     TEXT,
    received_count  INTEGER NOT NULL,
    accepted_count  INTEGER NOT NULL,
    rejected_count  INTEGER NOT NULL,
    as_of           TIMESTAMP NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS new_source.sales_landing (
    landing_id      BIGSERIAL PRIMARY KEY,
    source_key      TEXT NOT NULL,
    source_version  INTEGER NOT NULL,
    event_time      TIMESTAMP NOT NULL,
    load_batch_id   TEXT NOT NULL REFERENCES new_source.load_batches (load_batch_id),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    invoice_no      TEXT,
    stock_code      TEXT,
    description     TEXT,
    quantity        INTEGER,
    unit_price      DOUBLE PRECISION,
    currency        TEXT,
    customer_id     DOUBLE PRECISION,
    country         TEXT,
    discount        DOUBLE PRECISION,
    payment_method  TEXT,
    shipping_cost   DOUBLE PRECISION,
    category        TEXT,
    sales_channel   TEXT,
    return_status   TEXT,
    order_priority  TEXT,
    UNIQUE (source_key, source_version)
);
CREATE TABLE IF NOT EXISTS new_source.sales_rejects (
    reject_id       BIGSERIAL PRIMARY KEY,
    load_batch_id   TEXT NOT NULL REFERENCES new_source.load_batches (load_batch_id),
    row_number      INTEGER NOT NULL,
    reject_reason   TEXT NOT NULL,
    raw_record      JSONB,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (load_batch_id, row_number)
);
CREATE TABLE IF NOT EXISTS new_source.refresh_log (
    run_date        DATE PRIMARY KEY,
    airflow_run_id  TEXT,
    completed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

LANDING_COLUMNS = (
    "source_key", "source_version", "event_time", "load_batch_id", "invoice_no",
    "stock_code", "description", "quantity", "unit_price", "currency",
    "customer_id", "country", "discount", "payment_method", "shipping_cost",
    "category", "sales_channel", "return_status", "order_priority",
)


class EmptyBatchError(RuntimeError):
    """The batch contained no records and empty batches are not allowed."""


def connect_to_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "db"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "database"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "123456"),
    )


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_DDL)
    conn.commit()


def read_batch(path: str) -> tuple[list[dict], str]:
    """Return (rows, batch_id). batch_id is a content hash, so replays match."""
    with open(path, "rb") as handle:
        content = handle.read()
    batch_id = hashlib.sha256(content).hexdigest()[:16]
    rows = list(csv.DictReader(content.decode("utf-8-sig").splitlines()))
    return rows, batch_id


def split_batch(rows: list[dict], as_of: datetime):
    """Validate rows; return (accepted records, [(row_number, reason, raw)])."""
    accepted, rejected, seen = [], [], set()
    for number, raw in enumerate(rows, start=1):
        result = contract.validate_record(raw, as_of)
        if result.record is None:
            rejected.append((number, result.reasons[0], raw))
            continue
        key = (result.record["source_key"], result.record["source_version"])
        if key in seen:
            rejected.append((number, "duplicate_in_batch", raw))
            continue
        seen.add(key)
        accepted.append(result.record)
    return accepted, rejected


def load_batch(conn, path: str, as_of: datetime | None = None,
               allow_empty: bool = contract.ALLOW_EMPTY_BATCH) -> dict:
    """Load one batch atomically. Raises on any failure (nothing is committed)."""
    as_of = as_of or datetime.now(timezone.utc).replace(tzinfo=None)
    rows, batch_id = read_batch(path)
    if not rows and not allow_empty:
        raise EmptyBatchError(f"Batch {path} has no records")
    accepted, rejected = split_batch(rows, as_of)
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO new_source.load_batches
                   (load_batch_id, source_file, received_count, accepted_count, rejected_count, as_of)
                   VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (load_batch_id) DO NOTHING""",
                (batch_id, os.path.basename(path), len(rows), len(accepted), len(rejected), as_of),
            )
            if accepted:
                execute_values(
                    cur,
                    f"INSERT INTO new_source.sales_landing ({', '.join(LANDING_COLUMNS)}) VALUES %s "
                    "ON CONFLICT (source_key, source_version) DO NOTHING",
                    [tuple({**r, "load_batch_id": batch_id}[c] for c in LANDING_COLUMNS) for r in accepted],
                )
            if rejected:
                execute_values(
                    cur,
                    "INSERT INTO new_source.sales_rejects (load_batch_id, row_number, reject_reason, raw_record) "
                    "VALUES %s ON CONFLICT (load_batch_id, row_number) DO NOTHING",
                    [(batch_id, n, reason, Json(raw)) for n, reason, raw in rejected],
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"load_batch_id": batch_id, "received": len(rows),
            "accepted": len(accepted), "rejected": len(rejected)}


def resolve_path(ds: str | None = None) -> str:
    template = os.getenv("NEW_SOURCE_PATH", "data/new_source/sales_{ds}.csv")
    return template.format(ds=ds or datetime.now(timezone.utc).strftime("%Y-%m-%d"))


def main(ds: str | None = None) -> dict:
    """Airflow entry point. `ds` (YYYY-MM-DD) selects the daily file and as-of cutoff."""
    as_of = None
    if ds:
        as_of = datetime.strptime(ds, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    allow_empty = os.getenv("NEW_SOURCE_ALLOW_EMPTY", "false").lower() == "true"
    conn = connect_to_db()
    try:
        summary = load_batch(conn, resolve_path(ds), as_of, allow_empty)
    finally:
        conn.close()
    print(f"New source batch loaded: {json.dumps(summary)}")
    return summary
