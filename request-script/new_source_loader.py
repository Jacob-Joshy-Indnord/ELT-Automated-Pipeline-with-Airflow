"""Loader for the second (provisional) sales feed.

Unlike the legacy ``load_data`` module this loader is idempotent and raises on
failure so that the calling Airflow task fails. Source details are provisional
assumptions, see docs/new-source-contract.md.

Landing tables (schema ``new_source``):
  sales_landing  one row per accepted (source_key, source_version)
  sales_rejects  rejected records with batch id, row number and reason
  load_batches   one row per ingested batch (counts, load time)
"""
import csv
import hashlib
import json
import os
from datetime import datetime, timezone

SOURCE_FILE = os.environ.get("NEW_SOURCE_FILE", "data/raw/new_source_sales.csv")

REQUIRED_FIELDS = ["source_key", "event_time", "quantity", "unit_price"]
OPTIONAL_TEXT_FIELDS = [
    "description", "country", "payment_method", "category",
    "sales_channel", "return_status", "order_priority",
]
OPTIONAL_NUMERIC_FIELDS = ["discount", "shipping_cost"]
# Provisional: event_time is UTC; values are already in the legacy currency.
EXPECTED_COLUMNS = (
    REQUIRED_FIELDS + ["source_version"] + OPTIONAL_TEXT_FIELDS + OPTIONAL_NUMERIC_FIELDS
)

DDL = """
CREATE SCHEMA IF NOT EXISTS new_source;
CREATE TABLE IF NOT EXISTS new_source.load_batches (
    batch_id VARCHAR PRIMARY KEY,
    source_file VARCHAR,
    loaded_at TIMESTAMPTZ NOT NULL,
    accepted_count INTEGER NOT NULL,
    rejected_count INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS new_source.sales_landing (
    source_key VARCHAR NOT NULL,
    source_version INTEGER NOT NULL DEFAULT 1,
    event_time TIMESTAMP NOT NULL,
    batch_id VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    description VARCHAR,
    country VARCHAR,
    payment_method VARCHAR,
    category VARCHAR,
    sales_channel VARCHAR,
    return_status VARCHAR,
    order_priority VARCHAR,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC NOT NULL,
    discount NUMERIC,
    shipping_cost NUMERIC,
    PRIMARY KEY (source_key, source_version)
);
CREATE TABLE IF NOT EXISTS new_source.sales_rejects (
    batch_id VARCHAR NOT NULL,
    row_number INTEGER NOT NULL,
    reject_reason VARCHAR NOT NULL,
    raw_record JSONB,
    rejected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (batch_id, row_number)
);
"""


def batch_id_for(content: bytes) -> str:
    """Deterministic batch id: re-ingesting identical content is the same batch."""
    return hashlib.sha256(content).hexdigest()[:16]


def _blank(value):
    return value is None or str(value).strip() == ""


def validate_record(raw: dict):
    """Return (record, None) when accepted or (None, reason) when rejected."""
    for field in REQUIRED_FIELDS:
        if _blank(raw.get(field)):
            return None, f"missing required field: {field}"
    try:
        event_time = datetime.fromisoformat(raw["event_time"].strip())
    except ValueError:
        return None, "invalid event_time"
    if event_time.tzinfo is not None:  # normalise to naive UTC
        event_time = event_time.astimezone(timezone.utc).replace(tzinfo=None)
    try:
        quantity = int(str(raw["quantity"]).strip())
    except ValueError:
        return None, "invalid quantity"
    try:
        unit_price = float(raw["unit_price"])
    except ValueError:
        return None, "invalid unit_price"
    version = 1
    if not _blank(raw.get("source_version")):
        try:
            version = int(str(raw["source_version"]).strip())
        except ValueError:
            return None, "invalid source_version"
    record = {
        "source_key": raw["source_key"].strip(),
        "source_version": version,
        "event_time": event_time,
        "quantity": quantity,
        "unit_price": unit_price,
    }
    for field in OPTIONAL_TEXT_FIELDS:
        record[field] = None if _blank(raw.get(field)) else raw[field].strip()
    for field in OPTIONAL_NUMERIC_FIELDS:
        if _blank(raw.get(field)):
            record[field] = None
            continue
        try:
            record[field] = float(raw[field])
        except ValueError:
            return None, f"invalid {field}"
    return record, None


def parse_batch(content: bytes):
    """Parse a CSV batch into (accepted, rejects).

    Rejects are (row_number, reason, raw_record). Within a batch, an exact
    repeat of (source_key, source_version) is rejected as a duplicate.
    A missing header column fails the whole batch (ValueError).
    """
    reader = csv.DictReader(content.decode("utf-8-sig").splitlines())
    missing = [c for c in REQUIRED_FIELDS if c not in (reader.fieldnames or [])]
    if missing:
        raise ValueError(f"batch is missing required columns: {missing}")
    accepted, rejects, seen = [], [], set()
    for row_number, raw in enumerate(reader, start=1):
        record, reason = validate_record(raw)
        if record is None:
            rejects.append((row_number, reason, raw))
            continue
        key = (record["source_key"], record["source_version"])
        if key in seen:
            rejects.append((row_number, "duplicate source_key/source_version in batch", raw))
            continue
        seen.add(key)
        accepted.append(record)
    return accepted, rejects


UPSERT_SQL = """
INSERT INTO new_source.sales_landing (
    source_key, source_version, event_time, batch_id, loaded_at, description,
    country, payment_method, category, sales_channel, return_status,
    order_priority, quantity, unit_price, discount, shipping_cost
) VALUES (%(source_key)s, %(source_version)s, %(event_time)s, %(batch_id)s,
    %(loaded_at)s, %(description)s, %(country)s, %(payment_method)s,
    %(category)s, %(sales_channel)s, %(return_status)s, %(order_priority)s,
    %(quantity)s, %(unit_price)s, %(discount)s, %(shipping_cost)s)
ON CONFLICT (source_key, source_version) DO NOTHING
"""


def load_batch(conn, content: bytes, source_file: str = SOURCE_FILE):
    """Land a batch in one transaction. Replays are no-ops; errors propagate."""
    batch_id = batch_id_for(content)
    accepted, rejects = parse_batch(content)
    now = datetime.now(timezone.utc)
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            for record in accepted:
                cur.execute(UPSERT_SQL, {**record, "batch_id": batch_id, "loaded_at": now})
            for row_number, reason, raw in rejects:
                cur.execute(
                    """INSERT INTO new_source.sales_rejects
                       (batch_id, row_number, reject_reason, raw_record, rejected_at)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (batch_id, row_number) DO NOTHING""",
                    (batch_id, row_number, reason, json.dumps(raw), now),
                )
            cur.execute(
                """INSERT INTO new_source.load_batches
                   (batch_id, source_file, loaded_at, accepted_count, rejected_count)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (batch_id) DO UPDATE SET loaded_at = EXCLUDED.loaded_at""",
                (batch_id, source_file, now, len(accepted), len(rejects)),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"batch_id": batch_id, "accepted": len(accepted), "rejected": len(rejects)}


def connect():
    import psycopg2  # imported lazily so parsing can be used/tested without a driver
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "db"),
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ.get("DB_NAME", "database"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "123456"),
    )


def main(source_file: str = SOURCE_FILE):
    """Entry point for Airflow. Raises on a missing file, DB error or bad header."""
    with open(source_file, "rb") as handle:
        content = handle.read()
    conn = connect()
    try:
        result = load_batch(conn, content, source_file)
    finally:
        conn.close()
    print(f"New source batch loaded: {result}")
    return result
