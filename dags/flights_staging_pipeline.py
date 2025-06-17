from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime
import pandas as pd
from minio import Minio
import os
import json
import csv
import logging

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIG -----------------------------------------------------------------
incremental = Variable.get("incremental", default_var="True") == "True"
TABLES = Variable.get("tables_to_extract", deserialize_json=True)
TABLE_PK_MAP = Variable.get("tables_to_load", deserialize_json=True)

MINIO_CLIENT = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER", "minio"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
    secure=False,
)

BUCKET_NAME = "extracted-data"
if not MINIO_CLIENT.bucket_exists(BUCKET_NAME):
    MINIO_CLIENT.make_bucket(BUCKET_NAME)

JSON_COLUMNS = {
    "aircrafts_data": ["model"],
    "airports_data": ["city"],
}

# ---------------------------------------------------------------------------

def extract_table(table_name: str, **kwargs):
    logger.info("Extracting %s", table_name)
    hook = PostgresHook(postgres_conn_id="sources-conn")
    conn = hook.get_conn()

    if incremental:
        ds = kwargs["ds"]
        since = ds
        until = f"{ds} 23:59:59"
        query = f"SELECT * FROM bookings.{table_name} WHERE updated_at >= '{since}' AND updated_at <= '{until}'"
    else:
        query = f"SELECT * FROM bookings.{table_name}"

    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        raise AirflowSkipException("No new data")

    # stringify json columns
    for col in JSON_COLUMNS.get(table_name, []):
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)

    csv_path = f"/tmp/{table_name}.csv"
    df.to_csv(csv_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

    object_name = f"temp/{table_name}.csv"
    MINIO_CLIENT.fput_object(BUCKET_NAME, object_name, csv_path)
    os.remove(csv_path)


def load_table(table_name: str, **kwargs):
    logger.info("Loading %s", table_name)
    object_name = f"temp/{table_name}.csv"
    csv_path = f"/tmp/{table_name}.csv"
    MINIO_CLIENT.fget_object(BUCKET_NAME, object_name, csv_path)

    df = pd.read_csv(csv_path, keep_default_na=False, na_values=["NaN", ""])

    hook = PostgresHook(postgres_conn_id="warehouse-conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    if incremental:
        ds = kwargs["ds"]
        until = f"{ds} 23:59:59"
        cur.execute(f"DELETE FROM stg.{table_name} WHERE updated_at >= %s AND updated_at <= %s", (ds, until))
    else:
        cur.execute(f"DELETE FROM stg.{table_name}")

    # stringify json cols
    for col in JSON_COLUMNS.get(table_name, []):
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)

    cols = df.columns.tolist()
    from psycopg2.extras import execute_values
    execute_values(cur, f"INSERT INTO stg.{table_name} ({', '.join(cols)}) VALUES %s", [tuple(None if pd.isna(v) else v for v in row) for row in df.values])
    conn.commit()
    cur.close()
    conn.close()
    os.remove(csv_path)

# ---------------------------------------------------------------------------

def build_dag():
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
    }

    with DAG(
        dag_id="flights_staging_pipeline",
        default_args=default_args,
        schedule_interval="@daily",
        start_date=datetime(2025, 1, 1),
        catchup=True,
        max_active_runs=1,
        tags=["flights", "staging"],
    ) as dag:

        with TaskGroup(group_id="extract") as tg_extract:
            extract_ops = [
                PythonOperator(
                    task_id=f"extract_{tbl}",
                    python_callable=extract_table,
                    op_kwargs={"table_name": tbl},
                )
                for tbl in TABLES
            ]

        with TaskGroup(group_id="load") as tg_load:
            load_ops = []
            for idx, tbl in enumerate(TABLES):
                task = PythonOperator(
                    task_id=f"load_{tbl}",
                    python_callable=load_table,
                    op_kwargs={"table_name": tbl},
                    trigger_rule="all_success",
                )
                # ensure extract->load for same table
                extract_ops[idx] >> task
                load_ops.append(task)

            # chain loads sequentially for ordering
            for up, down in zip(load_ops, load_ops[1:]):
                up >> down

        trigger_warehouse = TriggerDagRunOperator(
            task_id="trigger_flights_warehouse_pipeline",
            trigger_dag_id="flights_warehouse_pipeline",
            wait_for_completion=False,
            reset_dag_run=True,
        )

        tg_extract >> tg_load >> trigger_warehouse

    return dag


globals()["flights_staging_pipeline"] = build_dag()
