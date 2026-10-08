"""Tests for the daily Shopify CSV pickup (task-Rp1VO6AIQyf)."""
import importlib
import sys
import types
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

import shopify_reader
from shopify_reader import business_date_for_run, read_shopify_batch, shopify_file_path

REPO_ROOT = Path(__file__).resolve().parents[1]

HEADER = "Name,Created at,Lineitem quantity,Lineitem price,Lineitem name\n"


def write_day(inbox: Path, d: date, body: str) -> Path:
    path = inbox / f"shopify_orders_{d:%Y-%m-%d}.csv"
    path.write_text(HEADER + body, encoding="utf-8")
    return path


# --- business date and path resolution -------------------------------------

def test_business_date_is_previous_utc_day():
    assert business_date_for_run(datetime(2026, 10, 8, 2, 0, tzinfo=timezone.utc)) == date(2026, 10, 7)


def test_business_date_converts_to_utc_first():
    # 2026-10-08 01:00 at UTC+05:00 is 2026-10-07 20:00 UTC -> D = 2026-10-06
    from datetime import timedelta
    tz = timezone(timedelta(hours=5))
    assert business_date_for_run(datetime(2026, 10, 8, 1, 0, tzinfo=tz)) == date(2026, 10, 6)


def test_business_date_rejects_naive_datetime():
    with pytest.raises(ValueError):
        business_date_for_run(datetime(2026, 10, 8))


def test_path_defaults_and_env_overrides(monkeypatch):
    monkeypatch.delenv("SHOPIFY_INBOX_DIR", raising=False)
    monkeypatch.delenv("SHOPIFY_FILE_PATTERN", raising=False)
    assert shopify_file_path(date(2026, 10, 7)) == Path("/opt/airflow/data/shopify/shopify_orders_2026-10-07.csv")

    monkeypatch.setenv("SHOPIFY_INBOX_DIR", "/in")
    monkeypatch.setenv("SHOPIFY_FILE_PATTERN", "orders_export_{date:%Y%m%d}.csv")
    assert shopify_file_path(date(2026, 10, 7)) == Path("/in/orders_export_20261007.csv")


# --- Scenario: Day's file is picked up --------------------------------------

def test_reads_only_the_file_for_d(tmp_path):
    d = date(2026, 10, 7)
    target = write_day(tmp_path, d, "#1001,2026-10-07 10:00:00 +0000,2,9.50,Mug\n")
    write_day(tmp_path, date(2026, 10, 6), "#0999,2026-10-06 10:00:00 +0000,1,1.00,Old\n")
    write_day(tmp_path, date(2026, 10, 8), "#1100,2026-10-08 10:00:00 +0000,1,1.00,New\n")
    (tmp_path / "shopify_orders_2026-10-07.csv.partial").write_text(HEADER + "#X,,1,1,Partial\n")

    batch = read_shopify_batch(d, inbox_dir=tmp_path)

    assert batch is not None
    assert batch.business_date == d
    assert batch.path == target
    assert batch.content == target.read_bytes()
    assert list(batch.rows["Name"]) == ["#1001"]


def test_values_are_kept_as_delivered_text(tmp_path):
    d = date(2026, 10, 7)
    write_day(tmp_path, d, "#1001,2026-10-07 10:00:00 +0000,02,9.50,\n")
    rows = read_shopify_batch(d, inbox_dir=tmp_path).rows
    assert rows.loc[0, "Lineitem quantity"] == "02"
    assert rows.loc[0, "Lineitem price"] == "9.50"
    assert rows.loc[0, "Lineitem name"] == ""


def test_utf8_bom_is_stripped_from_header(tmp_path):
    d = date(2026, 10, 7)
    path = tmp_path / f"shopify_orders_{d:%Y-%m-%d}.csv"
    path.write_bytes(b"\xef\xbb\xbf" + (HEADER + "#1001,2026-10-07 10:00:00 +0000,1,1.00,Mug\n").encode())
    assert "Name" in read_shopify_batch(d, inbox_dir=tmp_path).rows.columns


# --- Scenario: Missing file does not break existing ingestion ---------------

def test_missing_file_returns_none_without_raising(tmp_path, caplog):
    with caplog.at_level("WARNING"):
        assert read_shopify_batch(date(2026, 10, 7), inbox_dir=tmp_path) is None
    assert "shopify_orders_2026-10-07.csv" in caplog.text


def test_missing_inbox_directory_returns_none(tmp_path):
    assert read_shopify_batch(date(2026, 10, 7), inbox_dir=tmp_path / "absent") is None


def test_import_reads_no_files(monkeypatch, tmp_path):
    monkeypatch.setenv("SHOPIFY_INBOX_DIR", str(tmp_path / "absent"))
    calls = []
    monkeypatch.setattr("pandas.read_csv", lambda *a, **k: calls.append(a))
    importlib.reload(shopify_reader)
    assert calls == []


def test_existing_loader_imports_and_runs_without_shopify_file(monkeypatch, tmp_path):
    # Stub psycopg2 so the existing loader runs end-to-end with no database.
    executed = []

    class Cursor:
        def execute(self, sql, params=None):
            executed.append(params)

    class Conn:
        def cursor(self):
            return Cursor()

        def commit(self):
            pass

        def close(self):
            pass

    fake_pg = types.ModuleType("psycopg2")
    fake_pg.connect = lambda **kw: Conn()
    monkeypatch.setitem(sys.modules, "psycopg2", fake_pg)
    monkeypatch.setenv("SHOPIFY_INBOX_DIR", str(tmp_path / "absent"))
    monkeypatch.chdir(REPO_ROOT)  # load_data reads data/raw/... relative to CWD
    monkeypatch.delitem(sys.modules, "load_data", raising=False)

    import load_data
    load_data.main()

    inserts = [p for p in executed if p is not None]
    assert len(inserts) == len(load_data.df) > 0


# --- Scenario: Out-of-window orders are not dropped by the reader -----------

def test_out_of_window_rows_are_kept(tmp_path):
    d = date(2026, 10, 7)
    write_day(
        tmp_path,
        d,
        "#1000,2026-10-06 23:59:59 +0000,1,5.00,Late yesterday\n"
        "#1001,2026-10-07 12:00:00 +0000,1,5.00,In window\n"
        "#1002,2026-10-08 00:00:01 +0000,1,5.00,Tomorrow\n"
        "#1003,not-a-date,1,5.00,Garbage\n"
        "#1003,,1,5.00,Second line no timestamp\n",
    )
    rows = read_shopify_batch(d, inbox_dir=tmp_path).rows
    assert list(rows["Name"]) == ["#1000", "#1001", "#1002", "#1003", "#1003"]
    assert list(rows["Created at"]) == [
        "2026-10-06 23:59:59 +0000",
        "2026-10-07 12:00:00 +0000",
        "2026-10-08 00:00:01 +0000",
        "not-a-date",
        "",
    ]


def test_header_only_file_is_an_empty_batch_not_missing(tmp_path):
    d = date(2026, 10, 7)
    write_day(tmp_path, d, "")
    batch = read_shopify_batch(d, inbox_dir=tmp_path)
    assert batch is not None and len(batch.rows) == 0
