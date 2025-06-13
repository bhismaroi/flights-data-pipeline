"""Airflow DAG to extract data from source Postgres, land to MinIO and load into staging
schema, then trigger the warehouse DAG to perform transformations.

This DAG re-uses the extraction and loading logic previously implemented in
`flights_data_pipeline`.  We import those helper functions directly to avoid code
duplication.
"""
from __future__ import annotations

from datetime import datetime

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Re-use helpers from the existing pipeline file
from dags.flights_data_pipeline import extract_table, load_table  # type: ignore

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
}

# Variables that control which tables to process (already initialised in start.sh)
TABLES: list[str] = Variable.get("tables_to_extract", deserialize_json=True)
INCREMENTAL: bool = Variable.get("incremental", default_var="True") == "True"

with DAG(
    dag_id="flights_staging_pipeline",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["flights", "staging"],
) as dag:
    # Extract
    with TaskGroup(group_id="extract", tooltip="Extract from source to MinIO") as extract_group:
        extract_tasks = []
        for table in TABLES:
            t = PythonOperator(
                task_id=f"extract_{table}",
                python_callable=extract_table,
                op_kwargs={"table_name": table},
            )
            extract_tasks.append(t)

    # Load
    with TaskGroup(group_id="load", tooltip="Load from MinIO to staging") as load_group:
        load_tasks = []
        for idx, table in enumerate(TABLES):
            t = PythonOperator(
                task_id=f"load_{table}",
                python_callable=load_table,
                op_kwargs={"table_name": table},
                trigger_rule="all_success",
            )
            load_tasks.append(t)
            extract_tasks[idx] >> t  # dependency per table
        # Sequential order
        for u, d_ in zip(load_tasks, load_tasks[1:]):
            u >> d_

    # Trigger warehouse DAG
    trigger_warehouse = TriggerDagRunOperator(
        task_id="trigger_flights_warehouse_pipeline",
        trigger_dag_id="flights_warehouse_pipeline",
        wait_for_completion=False,
        reset_dag_run=True,
        execution_date="{{ ds }}",  # align with same logical date
    )

    extract_group >> load_group >> trigger_warehouse
