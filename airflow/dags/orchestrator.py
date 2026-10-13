import os
import sys
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

sys.path.append('/opt/airflow/request-script')
import pipeline_gate  # noqa: E402

# Host paths for the DockerOperator bind mounts come from configuration (Airflow Variable or
# environment), never hard-coded. They must be absolute paths on the Docker HOST.
def _cfg(name, default=None):
    return Variable.get(name, default_var=os.environ.get(name, default))

_THRESHOLDS = pipeline_gate.contract_thresholds()


def _run_date(ds):
    return date.fromisoformat(ds)


def ingest_legacy(ds, **_):
    from load_data import run_for_date  # raises on failure; skips run dates already loaded
    return run_for_date(_run_date(ds))


def ingest_new_source(ds, **_):
    import load_new_source
    return load_new_source.run(_run_date(ds), late_event_days=int(_cfg('new_source_late_event_days', _THRESHOLDS['late_event_days'])))


def declare_refresh_success(ds, **_):
    from load_new_source import connect
    cutoff = _cfg('new_source_daily_cutoff_utc', _THRESHOLDS['daily_cutoff_utc'])  # PROVISIONAL until owner approves
    conn = connect()
    try:
        pipeline_gate.declare_success(conn, _run_date(ds), cutoff)
    finally:
        conn.close()


default_args = {
    'description': 'A DAG to orchestrate data',
    'start_date': datetime(2025, 8, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': pipeline_gate.alert_failure,
}

dag = DAG(
    dag_id='online-sales-ingest-dbt-orchestrator',
    default_args=default_args,
    schedule=timedelta(days=1),
    catchup=False,
    max_active_runs=1,  # serialize reruns/backfills
)

with dag:
    ingest_legacy_task = PythonOperator(task_id='ingest_data', python_callable=ingest_legacy)
    ingest_new_task = PythonOperator(task_id='ingest_new_source', python_callable=ingest_new_source)

    # dbt build = run + tests; a nonzero exit fails the task (DockerOperator raises).
    data_transform = DockerOperator(
        task_id='transform_data_task',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command=pipeline_gate.dbt_command("{{ ds }}", _THRESHOLDS),  # templated by Airflow
        working_dir='/usr/app',
        mounts=[
            Mount(source=_cfg('DBT_HOST_PROJECT_DIR'), target='/usr/app', type='bind'),
            Mount(source=_cfg('DBT_HOST_PROFILES_FILE'), target='/root/.dbt/profiles.yml', type='bind'),
        ],
        network_mode=_cfg('DBT_DOCKER_NETWORK', 'online-sales-data-pipeline_my_network'),
        docker_url=_cfg('DOCKER_URL', 'unix://var/run/docker.sock'),
        mount_tmp_dir=False,
        auto_remove='success',
    )

    refresh_ok = PythonOperator(task_id='declare_refresh_success', python_callable=declare_refresh_success)

    [ingest_legacy_task, ingest_new_task] >> data_transform >> refresh_ok
