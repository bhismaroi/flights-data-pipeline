# dags/flights_warehouse_pipeline.py
"""DBT DAG that builds the dimensional warehouse using Astronomer Cosmos.

This DAG relies on the dbt project located at `/opt/airflow/dbt_flights`.  It
runs seeds, snapshots and models, emitting Airflow Datasets for each final
relation so that other DAGs can declare dependencies if desired.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from astronomer_cosmos.constants import LoadMode
from astronomer_cosmos.operators import DbtTaskGroup
from astronomer_cosmos.operators import DbtSnapshotOperator, DbtSeedOperator
from astronomer_cosmos.dataset import DbtDatasetFactory

# ---------------------------------------------------------------------------
# CONFIG --------------------------------------------------------------------
# ---------------------------------------------------------------------------

DBT_PROJECT_DIR = "/opt/airflow/dbt_flights"  # mounted into the webserver & workers
DBT_PROFILES_DIR = "/opt/airflow/dbt_flights"  # profiles.yml inside project root

# Models we expect to publish (emit datasets) - sync with diagram
FINAL_MODELS = [
    "dim_aircrafts",
    "dim_airport",
    "dim_passenger",
    "dim_seat",
    "fct_boarding_pass",
    "fct_booking_ticket",
    "fct_flight_activity",
    "fct_seat_occupied_daily",
]

# ---------------------------------------------------------------------------
# DAG DEFINITION ------------------------------------------------------------
# ---------------------------------------------------------------------------

def create_dataset(model):
    """Helper to build an Airflow Dataset object for a Postgres table."""
    return DbtDatasetFactory(  # type: ignore
        conn_id="warehouse-conn", target="warehouse", schema="final", name=model
    ).build()

with DAG(
    dag_id="flights_warehouse_pipeline",
    description="Builds the dimensional warehouse via dbt (seeds + snapshots + models)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # triggered from upstream DAG
    catchup=False,
    max_active_runs=1,
    tags=["flights", "warehouse", "dbt"],
) as dag:

    # ---------------------------------------------------------------------
    # SEEDS ----------------------------------------------------------------
    # ---------------------------------------------------------------------

    seeds = DbtSeedOperator(
        task_id="dbt_seed",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )

    # ---------------------------------------------------------------------
    # SNAPSHOTS ------------------------------------------------------------
    # ---------------------------------------------------------------------

    snapshots = DbtSnapshotOperator(
        task_id="dbt_snapshot",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )

    # ---------------------------------------------------------------------
    # MODELS (build all, but we emit datasets for finals) ------------------
    # ---------------------------------------------------------------------

    models_tg = DbtTaskGroup(
        group_id="dbt_models",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        dbt_args={"select": "tag:warehouse"},  # prefer tag selection, fallback all
        load_mode=LoadMode.DBT_RUN,
        emit_datasets=True,
    )

    # Attach dataset emission for each final model
    for model in FINAL_MODELS:
        models_tg >> create_dataset(model)

    # Dependencies
    seeds >> snapshots >> models_tg
