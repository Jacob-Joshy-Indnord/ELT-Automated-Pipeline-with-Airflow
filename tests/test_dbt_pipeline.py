import pytest

import load_new_source as loader
from conftest import FIXTURES, run_dbt, seed_legacy
from datetime import datetime

AS_OF = datetime(2025, 1, 10, 12, 0)
REPORT_COLUMNS = ["description", "country", "quantity", "unit_price", "discount", "payment_method",
                  "shipping_cost", "total_sales", "total_revenue", "category", "sales_channel",
                  "is_returned", "order_priority", "invoice_month", "invoice_year", "invoice_quarter"]


def q(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def build(db, batch=None):
    conn, name, sock = db
    seed_legacy(conn)
    if batch:
        loader.load_batch(conn, str(FIXTURES / batch), AS_OF)
    res = run_dbt(name, sock, "run")
    assert res.returncode == 0, res.stdout + res.stderr
    return conn, name, sock


def test_legacy_only_baseline_and_empty_new_source(db):
    conn, name, sock = db
    loader.ensure_schema(conn)
    conn.cursor().execute("insert into new_source.load_batches values ('b0','x',1,1,0,now(),now())")
    conn.commit()
    conn, name, sock = build(db)
    legacy_total = q(conn, "select count(*), sum(total_sales), sum(total_revenue) from staging.sales_report")
    assert legacy_total[0][0] == 2


def test_valid_batches_reconcile_and_tests_pass(db):
    conn, name, sock = build(db, "valid_batch.csv")
    assert [r[0] for r in q(conn, "select column_name from information_schema.columns "
                                  "where table_schema='staging' and table_name='sales_report' order by ordinal_position")] == REPORT_COLUMNS
    assert q(conn, "select count(*), sum(total_sales) from staging.sales_report")[0] == (5, 57.5 + 9 + 40 + 49.0 + 6.0)
    assert q(conn, "select count(*) from staging.stg_new_source_sales")[0][0] == 3
    # legacy rows unchanged
    assert q(conn, "select count(*), sum(total_sales) from staging.stg_online_sales")[0] == (2, 49.0 + 6.0)
    # new rows contribute to aggregate reports
    assert q(conn, "select sum(order_count) from staging.channel_sales_report")[0][0] == 5
    assert q(conn, "select sum(order_count) from staging.category_sales_report")[0][0] == 5
    assert q(conn, "select sum(order_count) from staging.product_sales_report")[0][0] == 5
    # latest version (12 units) wins, no double count of NS-1
    assert q(conn, "select quantity from staging.stg_new_source_sales where source_key='NS-1'")[0][0] == 12
    res = run_dbt(name, sock, "test")
    assert res.returncode == 0, res.stdout + res.stderr


def test_replay_and_rebuild_is_stable(db):
    conn, name, sock = build(db, "valid_batch.csv")
    loader.load_batch(conn, str(FIXTURES / "valid_batch.csv"), AS_OF)
    assert run_dbt(name, sock, "run").returncode == 0
    assert q(conn, "select count(*) from staging.sales_report")[0][0] == 5


def test_non_positive_rows_excluded_and_bad_batch_fails_health(db):
    conn, name, sock = build(db, "invalid_batch.csv")
    assert q(conn, "select count(*) from staging.stg_new_source_sales")[0][0] == 1  # only OK-1
    res = run_dbt(name, sock, "test")
    assert res.returncode != 0 and "new_source_batch_health" in res.stdout


def test_report_mismatch_fails_reconciliation(db):
    conn, name, sock = build(db, "valid_batch.csv")
    conn.cursor().execute("delete from staging.sales_report where description = 'Blue Pen'")
    conn.commit()
    res = run_dbt(name, sock, "test", "--select", "new_source_staging_to_report_reconciliation")
    assert res.returncode != 0


def test_staging_mismatch_fails_reconciliation(db):
    conn, name, sock = build(db, "valid_batch.csv")
    conn.cursor().execute("delete from staging.stg_new_source_sales where source_key = 'NS-2'")
    conn.commit()
    res = run_dbt(name, sock, "test", "--select", "new_source_landing_to_staging_reconciliation")
    assert res.returncode != 0


def test_pause_switch_hides_new_source(db):
    conn, name, sock = db
    seed_legacy(conn)
    loader.load_batch(conn, str(FIXTURES / "valid_batch.csv"), AS_OF)
    assert run_dbt(name, sock, "run", "--vars", "{publish_new_source: false}").returncode == 0
    assert q(conn, "select count(*) from staging.sales_report")[0][0] == 2


def test_empty_latest_batch_fails_health(db):
    conn, name, sock = db
    seed_legacy(conn)
    loader.load_batch(conn, str(FIXTURES / "empty_batch.csv"), AS_OF, allow_empty=True)
    assert run_dbt(name, sock, "run").returncode == 0
    res = run_dbt(name, sock, "test", "--select", "new_source_batch_health")
    assert res.returncode != 0
