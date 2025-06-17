from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from cosmos.providers.dbt.core.operators import DbtSeedOperator, DbtSnapshotOperator, DbtRunOperator
from airflow.datasets import Dataset

# Path where the dbt project will live inside the Airflow container
DBT_PROJECT_DIR = "/opt/airflow/dbt/flights_project"
PROFILE_DIR = DBT_PROJECT_DIR  # profiles.yml expected here

FINAL_TABLES = [
    "dim_aircrafts",
    "dim_airport",
    "dim_passenger",
    "dim_seat",
    "fct_boarding_pass",
    "fct_booking_ticket",
    "fct_flight_activity",
    "fct_seat_occupied_daily",
]

DATASETS = [Dataset(f"postgres://warehouse:5432/postgres.final.{tbl}") for tbl in FINAL_TABLES]

with DAG(
    dag_id="flights_warehouse_pipeline",
    description="Run dbt models for flights warehouse",
    schedule_interval=None,  # triggered from staging DAG
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["flights", "warehouse", "dbt"],
    emit_dag_id_as_tag=True,
) as dag:

    dbt_seed = DbtSeedOperator(
        task_id="dbt_seed",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=PROFILE_DIR,
        full_refresh=True,
        install_deps=True,
    )

    dbt_snapshot = DbtSnapshotOperator(
        task_id="dbt_snapshot",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=PROFILE_DIR,
    )

    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=PROFILE_DIR,
        outlets=DATASETS,
    )

    dbt_seed >> dbt_snapshot >> dbt_run
