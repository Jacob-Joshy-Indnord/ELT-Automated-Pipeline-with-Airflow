import logging
import os
import sys
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

sys.path.append('/opt/airflow/request-script')

# Host paths bind-mounted into the dbt container. Defaults keep the previous
# values; override per environment with these env vars (verify before enabling).
DBT_PROJECT_HOST_PATH = os.environ.get(
    'DBT_PROJECT_HOST_PATH', 'E:/Works/DE Project/online-sales-data-pipeline/dbt/my_project')
DBT_PROFILES_HOST_PATH = os.environ.get(
    'DBT_PROFILES_HOST_PATH', 'E:/Works/DE Project/online-sales-data-pipeline/dbt/profiles.yml')
DOCKER_NETWORK = os.environ.get('DBT_DOCKER_NETWORK', 'online-sales-data-pipeline_my_network')

# Provisional daily cutoff: new-source ingestion must finish within this window.
NEW_SOURCE_CUTOFF = timedelta(hours=int(os.environ.get('NEW_SOURCE_CUTOFF_HOURS', '6')))

# Pause switch: set Airflow Variable new_source_publication_enabled=false to
# keep new-source rows out of the reports (see docs/new-source-release.md).
DBT_VARS = "--vars '{new_source_enabled: {{ var.value.get(\"new_source_publication_enabled\", \"true\") }}}'"


def safe_main_callable():
    from load_data import main
    return main()


def legacy_ingest_raising():
    """Legacy CSV append (behaviour unchanged) but failures reach Airflow.

    load_data.main swallows exceptions, so call its helpers directly.
    """
    import load_data
    conn = load_data.connect_to_db()
    try:
        load_data.create_table_query(conn)
        load_data.load_data_to_db(load_data.df, conn)
    finally:
        conn.close()


def new_source_ingest():
    from airflow.models import Variable
    if Variable.get('new_source_publication_enabled', default_var='true').lower() == 'false':
        logging.warning('New source publication paused; skipping ingestion.')
        return None
    from new_source_loader import main
    return main()


def alert_on_failure(context):
    ti = context['task_instance']
    logging.error('ALERT: %s failed in run %s; daily refresh NOT complete. '
                  'See docs/new-source-release.md for pause/recovery.',
                  ti.task_id, context.get('run_id'))


default_args = {
    'description': 'A DAG to orchestrate data',
    'start_date': datetime(2025, 8, 20),
    'catchup': False,
    'on_failure_callback': alert_on_failure,
}

dag = DAG(
    dag_id='online-sales-ingest-dbt-orchestrator',
    default_args=default_args,
    schedule=timedelta(days=1),
    catchup=False,
    max_active_runs=1
)


def dbt_task(task_id, command):
    return DockerOperator(
        task_id=task_id,
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command=command,
        working_dir='/usr/app',
        mounts=[
            Mount(source=DBT_PROJECT_HOST_PATH,
                  target='/usr/app',
                  type='bind'),
            Mount(source=DBT_PROFILES_HOST_PATH,
                  target='/root/.dbt/profiles.yml',
                  type='bind')
        ],
        network_mode=DOCKER_NETWORK,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        auto_remove='success'
    )


with dag:
    # Task 1: legacy CSV feed (append semantics unchanged; failures now raise)
    ingest_data = PythonOperator(
        task_id='ingest_data',
        python_callable=legacy_ingest_raising
    )
    # Task 1b: new feed, idempotent upsert, raises on failure
    ingest_new_source = PythonOperator(
        task_id='ingest_new_source',
        python_callable=new_source_ingest,
        execution_timeout=NEW_SOURCE_CUTOFF
    )
    # Task 2
    data_transform = dbt_task('transform_data_task', f'run {DBT_VARS}')
    # Task 3: quality gate
    data_test = dbt_task('test_data_task', f'test {DBT_VARS}')
    # Task 4: only reached when both loads and all tests passed
    daily_refresh_complete = EmptyOperator(task_id='daily_refresh_complete')

    [ingest_data, ingest_new_source] >> data_transform >> data_test >> daily_refresh_complete
