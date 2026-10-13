"""Daily-refresh gate helpers: cutoff check, success declaration, failure alert, DAG config.

Pure Python (no Airflow import) so it can be unit tested; the DAG wires it in.
"""
import json
import logging
import os
from datetime import datetime, time, timedelta, timezone

log = logging.getLogger(__name__)
CONTRACT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "new_source_contract.json")


class CutoffMissed(Exception):
    pass


def contract_thresholds(path=CONTRACT_PATH):
    with open(path) as f:
        return json.load(f)["thresholds"]


def cutoff_deadline(run_date, cutoff_hhmm):
    """Deadline (UTC) = cutoff time of day on the day AFTER the data date (run_date).
    The daily schedule runs for run_date once that day is over."""
    h, m = (int(x) for x in cutoff_hhmm.split(":"))
    return datetime.combine(run_date + timedelta(days=1), time(h, m), tzinfo=timezone.utc)


def check_cutoff(run_date, cutoff_hhmm, now=None):
    now = now or datetime.now(timezone.utc)
    deadline = cutoff_deadline(run_date, cutoff_hhmm)
    if now > deadline:
        raise CutoffMissed(f"daily cutoff {deadline.isoformat()} missed for {run_date} (now {now.isoformat()})")


def declare_success(conn, run_date, cutoff_hhmm, now=None):
    """Record a successful refresh for run_date, unless the cutoff was missed (then raise, no record)."""
    check_cutoff(run_date, cutoff_hhmm, now)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS staging.daily_refresh_log "
                "(run_date DATE PRIMARY KEY, declared_at TIMESTAMP NOT NULL DEFAULT now())")
    cur.execute("INSERT INTO staging.daily_refresh_log (run_date) VALUES (%s) "
                "ON CONFLICT (run_date) DO UPDATE SET declared_at = now()", (run_date,))
    conn.commit()


def alert_failure(context):
    """Airflow on_failure_callback. Logs at ERROR; add an email/webhook hook here once the
    alert channel is agreed (not evidenced in the source)."""
    ti = context.get("task_instance")
    log.error("ALERT: daily refresh failed: dag=%s task=%s run=%s",
              getattr(ti, "dag_id", "?"), getattr(ti, "task_id", "?"), context.get("ds"))


def dbt_command(run_date, thresholds):
    """run_date may be a date or an Airflow template string such as "{{ ds }}"."""
    vars_ = {"new_source_run_date": str(run_date),
             "new_source_zero_row_policy": thresholds["zero_row_policy"],
             "new_source_late_event_days": thresholds["late_event_days"]}
    return f"build --vars '{json.dumps(vars_)}'"
