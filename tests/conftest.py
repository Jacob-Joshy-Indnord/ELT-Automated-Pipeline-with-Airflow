import os
import subprocess
import sys
import tempfile
from pathlib import Path

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures" / "new_source"
sys.path.insert(0, str(ROOT / "request-script"))
os.chdir(ROOT)  # legacy load_data reads a relative CSV at import time


@pytest.fixture(scope="session")
def pg_server():
    pgserver = pytest.importorskip("pgserver")
    server = pgserver.get_server(tempfile.mkdtemp(prefix="pg_"))
    yield server
    server.cleanup()


@pytest.fixture()
def db(pg_server, request):
    """A fresh database per test, returned as (conn, dbname, socket_dir)."""
    name = "t_" + str(abs(hash(request.node.nodeid)))
    socket_dir = pg_server.get_uri().split("host=")[1]
    admin = psycopg2.connect(host=socket_dir, user="postgres", dbname="postgres")
    admin.autocommit = True
    admin.cursor().execute(f'DROP DATABASE IF EXISTS "{name}"')
    admin.cursor().execute(f'CREATE DATABASE "{name}"')
    admin.close()
    conn = psycopg2.connect(host=socket_dir, user="postgres", dbname=name)
    yield conn, name, socket_dir
    conn.close()


def seed_legacy(conn):
    """Create the legacy staging table and insert 3 rows (1 excluded by qty <= 0)."""
    import load_data
    load_data.create_table_query(conn)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO staging.online_sales_data
        (invoice_no, stock_code, description, quantity, invoice_date, unit_price, discount,
         payment_method, shipping_cost, category, sales_channel, return_status, order_priority, country)
        VALUES
        ('1','A','Legacy Mug',5,'2024-05-01 10:00',10.0,1.0,'paypall',2.0,'Apparel','Online','Not Returned','High','Spain'),
        ('2','B','Legacy Pen',2,'2024-06-01 10:00',3.0,0,'Credit Card',NULL,'Stationery','In-store','Returned','Low','France'),
        ('3','C','Legacy Bad',0,'2024-06-01 10:00',3.0,0,'Credit Card',NULL,'Stationery','In-store','Returned','Low','France')
    """)
    conn.commit()


def run_dbt(dbname, socket_dir, *args):
    env = {**os.environ, "DBT_HOST": socket_dir, "DBT_DBNAME": dbname, "DBT_USER": "postgres",
           "DBT_PASSWORD": "", "DBT_PORT": "5432"}
    return subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", *args, "--project-dir", "dbt/my_project", "--profiles-dir", "dbt"],
        cwd=ROOT, env=env, capture_output=True, text=True,
    )
