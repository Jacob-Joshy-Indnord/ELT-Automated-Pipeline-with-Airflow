import ast
import os
import sys
from datetime import date, datetime, timezone

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "request-script"))
sys.path.insert(0, os.path.join(ROOT, "airflow", "scripts"))
import pipeline_gate as pg  # noqa: E402
import docker_preflight  # noqa: E402

RUN = date(2026, 1, 10)


class Conn:
    def __init__(self):
        self.sql, self.committed = [], False

    def cursor(self):
        return self

    def execute(self, sql, params=None):
        self.sql.append(sql)

    def commit(self):
        self.committed = True


def test_cutoff_met_declares_success():
    conn = Conn()
    pg.declare_success(conn, RUN, "06:00", now=datetime(2026, 1, 11, 5, 59, tzinfo=timezone.utc))
    assert conn.committed and any("daily_refresh_log" in s for s in conn.sql)


def test_cutoff_missed_blocks_success_and_raises():
    conn = Conn()
    with pytest.raises(pg.CutoffMissed):
        pg.declare_success(conn, RUN, "06:00", now=datetime(2026, 1, 11, 6, 1, tzinfo=timezone.utc))
    assert not conn.committed and not conn.sql


def test_dbt_command_passes_thresholds_and_run_date():
    cmd = pg.dbt_command("{{ ds }}", {"zero_row_policy": "fail", "late_event_days": 1})
    assert cmd.startswith("build --vars") and "{{ ds }}" in cmd and '"fail"' in cmd


def test_preflight_requires_configured_mounts():
    assert len(docker_preflight.check_config({})) == 2
    assert docker_preflight.check_config({"DBT_HOST_PROJECT_DIR": "/a", "DBT_HOST_PROFILES_FILE": "/b"}) == []


def test_dag_has_no_hardcoded_host_paths_and_explicit_settings():
    src = open(os.path.join(ROOT, "airflow", "dags", "orchestrator.py")).read()
    assert "E:/" not in src and "E:\\" not in src
    ast.parse(src)
    for needle in ("catchup=False", "max_active_runs=1", "'retries'", "on_failure_callback", "declare_refresh_success"):
        assert needle in src
    assert "[ingest_legacy_task, ingest_new_task] >> data_transform >> refresh_ok" in src


# --- legacy replay guard -------------------------------------------------------------------
sys.modules.setdefault("psycopg2", type(sys)("psycopg2"))
import load_data  # noqa: E402


class LegacyConn:
    def __init__(self, logged=()):
        self.logged, self.inserted, self.committed, self.rolled_back = set(logged), 0, 0, 0
        self._row = None

    def cursor(self):
        return self

    def execute(self, sql, params=None):
        if "legacy_load_log" in sql and sql.lstrip().startswith("INSERT"):
            fresh = params[0] not in self.logged
            self.logged.add(params[0])
            self._row = (params[0],) if fresh else None

    def fetchone(self):
        return self._row

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1


def test_legacy_guard_loads_once_per_run_date(monkeypatch):
    loads = []
    monkeypatch.setattr(load_data, "load_data_to_db", lambda frame, conn: loads.append(len(frame)))
    monkeypatch.setattr(load_data, "create_table_query", lambda conn: None)
    conn = LegacyConn()
    assert load_data.run_for_date(RUN, conn=conn, data=[1, 2, 3]) == 3
    assert load_data.run_for_date(RUN, conn=conn, data=[1, 2, 3]) == 0  # rerun/backfill skipped
    assert loads == [3]
    assert load_data.run_for_date(date(2026, 1, 11), conn=conn, data=[1]) == 1


def test_legacy_failure_propagates(monkeypatch):
    def boom(frame, conn):
        raise RuntimeError("insert failed")
    monkeypatch.setattr(load_data, "load_data_to_db", boom)
    monkeypatch.setattr(load_data, "create_table_query", lambda conn: None)
    conn = LegacyConn()
    with pytest.raises(RuntimeError):
        load_data.run_for_date(RUN, conn=conn, data=[1])
    assert conn.rolled_back == 1
