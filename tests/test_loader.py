from datetime import datetime

import pytest

import load_new_source as loader
from conftest import FIXTURES

AS_OF = datetime(2025, 1, 10, 12, 0)


def count(conn, table):
    cur = conn.cursor()
    cur.execute(f"select count(*) from new_source.{table}")
    return cur.fetchone()[0]


def test_replay_does_not_duplicate(db):
    conn = db[0]
    first = loader.load_batch(conn, str(FIXTURES / "valid_batch.csv"), AS_OF)
    second = loader.load_batch(conn, str(FIXTURES / "valid_batch.csv"), AS_OF)
    assert first == second and first["accepted"] == 4
    assert count(conn, "sales_landing") == 4 and count(conn, "load_batches") == 1


def test_rejects_traceable_and_replay_safe(db):
    conn = db[0]
    summary = loader.load_batch(conn, str(FIXTURES / "invalid_batch.csv"), AS_OF)
    loader.load_batch(conn, str(FIXTURES / "invalid_batch.csv"), AS_OF)
    cur = conn.cursor()
    cur.execute("select row_number, reject_reason, load_batch_id from new_source.sales_rejects order by 1")
    rows = cur.fetchall()
    assert [r[1] for r in rows] == [
        "duplicate_in_batch", "missing_required:source_key", "invalid_number:quantity",
        "invalid_timestamp", "invalid_value:category", "unsupported_currency",
        "late_arrival", "future_event_time"]
    assert {r[2] for r in rows} == {summary["load_batch_id"]}
    assert summary["accepted"] == 3 and summary["rejected"] == 8
    assert count(conn, "sales_landing") == 3


def test_empty_batch_raises_unless_allowed(db):
    conn = db[0]
    with pytest.raises(loader.EmptyBatchError):
        loader.load_batch(conn, str(FIXTURES / "empty_batch.csv"), AS_OF)
    assert loader.load_batch(conn, str(FIXTURES / "empty_batch.csv"), AS_OF, allow_empty=True)["received"] == 0


def test_missing_file_raises(db):
    with pytest.raises(FileNotFoundError):
        loader.load_batch(db[0], "/nonexistent.csv", AS_OF)


def test_database_failure_is_raised_and_rolled_back(db):
    conn = db[0]
    loader.ensure_schema(conn)
    cur = conn.cursor()
    cur.execute("alter table new_source.sales_landing drop column description")
    conn.commit()
    with pytest.raises(Exception):
        loader.load_batch(conn, str(FIXTURES / "valid_batch.csv"), AS_OF)
    assert count(conn, "load_batches") == 0


def test_main_raises_on_connection_failure(monkeypatch):
    monkeypatch.setenv("NEW_SOURCE_PATH", str(FIXTURES / "valid_batch.csv"))
    monkeypatch.setenv("DB_HOST", "/nonexistent-host")
    with pytest.raises(Exception):
        loader.main("2025-01-10")


def test_new_version_supersedes_in_landing_as_separate_row(db):
    conn = db[0]
    loader.load_batch(conn, str(FIXTURES / "valid_batch.csv"), AS_OF)
    cur = conn.cursor()
    cur.execute("select source_version from new_source.sales_landing where source_key='NS-1' order by 1")
    assert [r[0] for r in cur.fetchall()] == [1, 2]
