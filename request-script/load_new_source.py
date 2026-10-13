"""Idempotent landing of the new sales source (PROVISIONAL contract, see new_source_contract.json).

Lands accepted rows in landing_new.new_sales_source (upsert on source_key, version) and
rejected rows in landing_new.new_sales_rejects. Replaying a batch is a no-op. Any failure
is raised so the calling Airflow task fails. The legacy CSV loader is untouched.
"""
import csv
import json
import math
import os
from datetime import datetime, timedelta

REQUIRED = ("source_key", "version", "event_time", "quantity", "unit_price")
OPTIONAL = ("stock_code", "description", "customer_id", "country", "discount", "payment_method",
            "shipping_cost", "category", "sales_channel", "return_status", "shipment_provider",
            "warehouse_location", "order_priority")
ACCEPTED_COLUMNS = REQUIRED + OPTIONAL

DDL = """
CREATE SCHEMA IF NOT EXISTS landing_new;
CREATE TABLE IF NOT EXISTS landing_new.new_sales_source (
    source_key VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    event_time TIMESTAMP NOT NULL,
    load_batch_id VARCHAR NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DOUBLE PRECISION NOT NULL,
    stock_code VARCHAR, description VARCHAR, customer_id DOUBLE PRECISION, country VARCHAR,
    discount DOUBLE PRECISION, payment_method VARCHAR, shipping_cost DOUBLE PRECISION,
    category VARCHAR, sales_channel VARCHAR, return_status VARCHAR, shipment_provider VARCHAR,
    warehouse_location VARCHAR, order_priority VARCHAR,
    loaded_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (source_key, version)
);
CREATE TABLE IF NOT EXISTS landing_new.new_sales_rejects (
    load_batch_id VARCHAR NOT NULL,
    row_number INTEGER NOT NULL,
    source_key VARCHAR,
    reject_reason VARCHAR NOT NULL,
    raw_record JSONB,
    PRIMARY KEY (load_batch_id, row_number)
);
CREATE TABLE IF NOT EXISTS landing_new.load_batches (
    load_batch_id VARCHAR PRIMARY KEY,
    run_date DATE NOT NULL,
    rows_received INTEGER NOT NULL,
    rows_accepted INTEGER NOT NULL,
    rows_rejected INTEGER NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT now()
);
"""


def _text(v):
    v = "" if v is None else str(v).strip()
    return v or None


def _num(v, cast):
    f = float(v)
    if math.isnan(f) or math.isinf(f):
        raise ValueError("not finite")
    if cast is int:
        if f != int(f):
            raise ValueError("not integer")
        return int(f)
    return f


def validate_row(raw, run_date, late_event_days):
    """Return (clean_row, None) or (None, reject_reason). Provisional contract: UTC, USD, no conversion."""
    row = {k: _text(raw.get(k)) for k in ACCEPTED_COLUMNS}
    missing = [k for k in REQUIRED if row[k] is None]
    if missing:
        return None, "missing required field: " + ",".join(missing)
    try:
        row["version"] = _num(row["version"], int)
        row["quantity"] = _num(row["quantity"], int)
        row["unit_price"] = _num(row["unit_price"], float)
    except ValueError:
        return None, "malformed number in version/quantity/unit_price"
    try:
        row["event_time"] = datetime.fromisoformat(row["event_time"])
    except ValueError:
        return None, "malformed event_time"
    for k in ("customer_id", "discount", "shipping_cost"):
        if row[k] is not None:
            try:
                row[k] = _num(row[k], float)
            except ValueError:
                return None, f"malformed number in {k}"
    if row["event_time"].date() > run_date:
        return None, "event_time after run date"
    if row["event_time"].date() < run_date - timedelta(days=late_event_days):
        return None, f"late event (> {late_event_days} day(s) before run date)"
    return row, None


def split_batch(records, run_date, late_event_days=1):
    """Validate records; returns (accepted, rejects). Duplicate (source_key, version) inside the
    batch keep the first occurrence; repeats are rejected as duplicates."""
    accepted, rejects, seen = [], [], set()
    for n, raw in enumerate(records, start=1):
        row, reason = validate_row(raw, run_date, late_event_days)
        if row is not None:
            k = (row["source_key"], row["version"])
            if k in seen:
                row, reason = None, "duplicate source_key/version in batch"
            else:
                seen.add(k)
        if row is None:
            rejects.append({"row_number": n, "source_key": _text(raw.get("source_key")),
                            "reject_reason": reason, "raw_record": {k: v for k, v in raw.items() if k}})
        else:
            accepted.append(row)
    return accepted, rejects


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def batch_id_for(run_date):
    return f"new-source-{run_date.isoformat()}"


def load_batch(conn, records, run_date, late_event_days=1, batch_id=None):
    """Land one batch atomically and idempotently. Raises on any failure (after rollback)."""
    batch_id = batch_id or batch_id_for(run_date)
    accepted, rejects = split_batch(records, run_date, late_event_days)
    try:
        cur = conn.cursor()
        cur.execute(DDL)
        cur.execute("DELETE FROM landing_new.new_sales_rejects WHERE load_batch_id = %s", (batch_id,))
        cols = ",".join(ACCEPTED_COLUMNS)
        marks = ",".join(["%s"] * (len(ACCEPTED_COLUMNS) + 1))
        updates = ",".join(f"{c}=EXCLUDED.{c}" for c in ACCEPTED_COLUMNS if c not in ("source_key", "version"))
        for r in accepted:
            cur.execute(
                f"INSERT INTO landing_new.new_sales_source ({cols}, load_batch_id) VALUES ({marks}) "
                f"ON CONFLICT (source_key, version) DO UPDATE SET {updates}, load_batch_id=EXCLUDED.load_batch_id",
                tuple(r[c] for c in ACCEPTED_COLUMNS) + (batch_id,))
        for r in rejects:
            cur.execute(
                "INSERT INTO landing_new.new_sales_rejects (load_batch_id, row_number, source_key, reject_reason, raw_record) "
                "VALUES (%s,%s,%s,%s,%s)",
                (batch_id, r["row_number"], r["source_key"], r["reject_reason"], json.dumps(r["raw_record"])))
        cur.execute(
            "INSERT INTO landing_new.load_batches (load_batch_id, run_date, rows_received, rows_accepted, rows_rejected) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (load_batch_id) DO UPDATE SET rows_received=EXCLUDED.rows_received, "
            "rows_accepted=EXCLUDED.rows_accepted, rows_rejected=EXCLUDED.rows_rejected, loaded_at=now()",
            (batch_id, run_date, len(records), len(accepted), len(rejects)))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"batch_id": batch_id, "received": len(records), "accepted": len(accepted), "rejected": len(rejects)}


def connect():
    import psycopg2
    return psycopg2.connect(host=os.environ.get("PGHOST", "db"), port=int(os.environ.get("PGPORT", 5432)),
                            database=os.environ.get("PGDATABASE", "database"),
                            user=os.environ.get("PGUSER", "postgres"),
                            password=os.environ.get("PGPASSWORD", "123456"))


def run(run_date, base_dir=None, late_event_days=1):
    """Airflow entry point. A missing file or any DB error raises (fails the task)."""
    base_dir = base_dir or os.environ.get("NEW_SOURCE_DIR", "data/new_source")
    path = os.path.join(base_dir, f"{run_date.isoformat()}.csv")
    records = read_csv(path)  # FileNotFoundError => task fails (no silent empty day)
    conn = connect()
    try:
        return load_batch(conn, records, run_date, late_event_days)
    finally:
        conn.close()
