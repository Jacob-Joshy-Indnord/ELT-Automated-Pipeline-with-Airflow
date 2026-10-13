import pandas as pd
import psycopg2

def connect_to_db():
    print("Connecting to the PostgresSQL database...")
    try:
        conn = psycopg2.connect(
            host="db",
            port=5432,
            database="database",
            user="postgres",
            password="123456"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

def create_table_query(conn):
    print("Creating table if it does not exist...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS staging;
            CREATE TABLE IF NOT EXISTS staging.online_sales_data (
                id SERIAL PRIMARY KEY,
                invoice_no VARCHAR,
                stock_code VARCHAR,
                description VARCHAR,
                quantity INTEGER,
                invoice_date TIMESTAMP,
                unit_price FLOAT,
                customer_id FLOAT,
                country VARCHAR,
                discount FLOAT,
                payment_method VARCHAR,
                shipping_cost FLOAT,
                category VARCHAR,
                sales_channel VARCHAR,
                return_status VARCHAR,
                shipment_provider VARCHAR,
                warehouse_location VARCHAR,
                order_priority VARCHAR
            );
        """)
        conn.commit()
        print("Table created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
        raise

def get_df():
    return pd.read_csv(r'data/raw/online_sales_dataset.csv')

def load_data_to_db(df, conn):
    print("Loading online sales data into the database...")
    try:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO staging.online_sales_data (
                    invoice_no, stock_code, description, quantity, invoice_date, unit_price,
                    customer_id, country, discount, payment_method, shipping_cost, category,
                    sales_channel, return_status, shipment_provider, warehouse_location, order_priority
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))
        conn.commit()
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data into the database: {e}")
        raise

def run_for_date(run_date, conn=None, data=None):
    """Guarded, failure-propagating entry point used by the Airflow DAG.

    Appends the legacy CSV at most once per run date (the loader itself still appends without
    dedup). The log row and the data commit together, so a failed load can be retried and a
    completed run date is skipped on rerun/backfill. Raises on any error. Returns rows loaded.
    """
    own_conn = conn is None
    conn = conn or connect_to_db()
    try:
        create_table_query(conn)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging.legacy_load_log (
                run_date DATE PRIMARY KEY,
                loaded_at TIMESTAMP NOT NULL DEFAULT now()
            )
        """)
        cursor.execute(
            "INSERT INTO staging.legacy_load_log (run_date) VALUES (%s) ON CONFLICT DO NOTHING RETURNING run_date",
            (run_date,))
        if cursor.fetchone() is None:
            conn.commit()
            print(f"Legacy rows already loaded for {run_date}; skipping.")
            return 0
        frame = get_df() if data is None else data
        load_data_to_db(frame, conn)  # commits the log row together with the data
        return len(frame)
    except Exception:
        conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()

def main():
    try:
        conn = connect_to_db()
        create_table_query(conn)
        load_data_to_db(get_df(), conn)
    except Exception as e:
        print(f"An error occurred during execution: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")