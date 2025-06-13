# dags/flights_staging_pipeline.py
"""Airflow DAG to extract data from the source DB, load it into the staging
schema, then trigger the DBT warehouse pipeline DAG.

It re-uses the same extraction and loading logic that already exists in the
legacy `flights_data_pipeline` DAG, but removes the DB-level SQL transforms and
instead fires `flights_warehouse_pipeline` once loading finishes.
"""
from __future__ import annotations

from datetime import datetime
import json
import logging
import os
import csv

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.minio.hooks.minio import MinioHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from minio import Minio

# ---------------------------------------------------------------------------
# GLOBALS / HELPERS
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Slack failure callback -----------------------------------------------------

def slack_fail(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    exception = context.get("exception")

    message = (
        f":red_circle: *Airflow Alert!*\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Execution Date*: {execution_date}\n"
        f"*Log*: <{log_url}|View Logs>\n"
        f"*Exception*: `{exception}`"
    )

    SlackWebhookOperator(
        task_id="notify_slack_failure",
        http_conn_id="slack_notifier",
        message=message,
    ).execute(context=context)


# ---------------------------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "on_failure_callback": slack_fail,
}

with DAG(
    dag_id="flights_staging_pipeline",
    description="Extract + load staging tables then trigger warehouse DBT DAG",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["flights", "staging"],
) as dag:

    # ---------------------------------------------------------------------
    # CONFIGURATION (Airflow Variables & connections)
    # ---------------------------------------------------------------------

    incremental = Variable.get("incremental", default_var="True") == "True"
    TABLES = Variable.get("tables_to_extract", deserialize_json=True)

    minio_client = Minio(
        "minio:9000",
        access_key=os.getenv("MINIO_ROOT_USER", "minio"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
        secure=False,
    )

    BUCKET_NAME = "extracted-data"
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)

    # Declare JSON columns per table (used to convert JSON objects to strings)
    JSON_COLUMNS = {
        "aircrafts_data": ["model"],
        "airports_data": ["city"],
    }

    # Directory that holds SQL bootstrap scripts
    SQL_DIR = "/opt/airflow/include"

    # ---------------------------------------------------------------------
    # INITIALISE DB SCHEMAS -----------------------------------------------
    # ---------------------------------------------------------------------
    with TaskGroup(group_id="init_db", tooltip="Create DB schemas if absent") as init_group:
        source_init = PostgresOperator(
            task_id="source_init",
            postgres_conn_id="warehouse-conn",
            sql=f"{SQL_DIR}/source_init.sql",
        )

        staging_schema = PostgresOperator(
            task_id="staging_schema",
            postgres_conn_id="warehouse-conn",
            sql=f"{SQL_DIR}/staging_schema.sql",
        )

        warehouse_init = PostgresOperator(
            task_id="warehouse_init",
            postgres_conn_id="warehouse-conn",
            sql=f"{SQL_DIR}/warehouse_init.sql",
        )

        source_init >> staging_schema >> warehouse_init

    # ---------------------------------------------------------------------
    # EXTRACT TASK-GROUP ----------------------------------------------------
    # ---------------------------------------------------------------------

    def extract_table(table_name: str, **kwargs):
        """Pull a table from the `bookings` schema and upload as CSV to MinIO."""
        logger.info("Extracting %s", table_name)
        hook = PostgresHook(postgres_conn_id="sources-conn")
        conn = hook.get_conn()

        if incremental:
            ds = kwargs["ds"]  # YYYY-MM-DD
            since = ds
            until = f"{ds} 23:59:59"
            query = (
                f"SELECT * FROM bookings.{table_name} "
                f"WHERE updated_at >= '{since}' AND updated_at <= '{until}'"
            )
        else:
            query = f"SELECT * FROM bookings.{table_name}"

        df = pd.read_sql(query, conn)
        conn.close()

        # Skip completely if nothing to do (for airflow *and* downstream tasks)
        if df.empty:
            raise AirflowSkipException(f"No new data for {table_name}")

        # Handle JSON columns --> ensure serialisable in CSV
        json_cols = JSON_COLUMNS.get(table_name, [])
        for col in json_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)

        csv_path = f"/tmp/{table_name}.csv"
        df.to_csv(csv_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

        object_name = f"temp/{table_name}.csv"
        minio_client.fput_object(BUCKET_NAME, object_name, csv_path)
        logger.info("Uploaded %s to MinIO (%s)", csv_path, object_name)
        os.remove(csv_path)

    with TaskGroup(group_id="extract", tooltip="Extract tables to MinIO") as extract_group:
        extract_tasks = []
        for tbl in TABLES:
            extract_tasks.append(
                PythonOperator(
                    task_id=f"extract_{tbl}",
                    python_callable=extract_table,
                    op_kwargs={"table_name": tbl},
                )
            )

    # ---------------------------------------------------------------------
    # LOAD TASK-GROUP -------------------------------------------------------
    # ---------------------------------------------------------------------

    def load_table(table_name: str, **kwargs):
        """Load a CSV from MinIO into the `stg` schema (incremental aware)."""
        logger.info("Loading %s into staging", table_name)

        object_name = f"temp/{table_name}.csv"
        csv_path = f"/tmp/{table_name}.csv"
        minio_client.fget_object(BUCKET_NAME, object_name, csv_path)
        df = pd.read_csv(csv_path, keep_default_na=False, na_values=["NaN", ""])

        hook = PostgresHook(postgres_conn_id="warehouse-conn")
        conn = hook.get_conn()
        cur = conn.cursor()

        # Clean existing slice / whole table depending on incremental flag
        if incremental:
            ds = kwargs["ds"]
            until = f"{ds} 23:59:59"
            cur.execute(
                f"DELETE FROM stg.{table_name} WHERE updated_at >= %s AND updated_at <= %s",
                (ds, until),
            )
        else:
            cur.execute(f"TRUNCATE stg.{table_name}")

        # Prepare values (JSON columns stringified)
        json_cols = JSON_COLUMNS.get(table_name, [])
        for col in json_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)

        columns = df.columns.tolist()
        from psycopg2.extras import execute_values

        values = [tuple(None if pd.isna(v) else v for v in row) for row in df.values]
        insert_sql = f"INSERT INTO stg.{table_name} ({', '.join(columns)}) VALUES %s"
        execute_values(cur, insert_sql, values)

        conn.commit()
        cur.close()
        conn.close()
        os.remove(csv_path)
        logger.info("Loaded %s rows into stg.%s", len(values), table_name)

    with TaskGroup(group_id="load", tooltip="Load files from MinIO to staging") as load_group:
        load_tasks = []
        for idx, tbl in enumerate(TABLES):
            t = PythonOperator(
                task_id=f"load_{tbl}",
                python_callable=load_table,
                op_kwargs={"table_name": tbl},
                trigger_rule="all_success",
            )
            load_tasks.append(t)
            # wire 1-to-1 deps within groups
            extract_tasks[idx] >> t

        # maintain sequential order amongst load tasks to avoid DB saturation
        for up, down in zip(load_tasks, load_tasks[1:]):
            up >> down

    # ---------------------------------------------------------------------
    # TRIGGER DOWNSTREAM DBT DAG -------------------------------------------
    # ---------------------------------------------------------------------

    trigger_warehouse = TriggerDagRunOperator(
        task_id="trigger_flights_warehouse_pipeline",
        trigger_dag_id="flights_warehouse_pipeline",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Overall DAG dependencies
    init_group >> extract_group >> load_group >> trigger_warehouse
