# dags/flights_data_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pandas as pd
from minio import Minio
import psycopg2
from psycopg2.extras import execute_values
import os
import json
import logging
import csv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Slack failure callback
def slack_fail(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    exception = context.get('exception')

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

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'on_failure_callback': slack_fail,
}

# Initialize the DAG
with DAG(
    dag_id='flights_data_pipeline',
    default_args=default_args,
    description='A data pipeline for flight booking system',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=1,
) as dag:

    # Configuration using Airflow Variables and Connections
    incremental = Variable.get("incremental", default_var="True") == "True"
    TABLES = Variable.get("tables_to_extract", deserialize_json=True)
    TABLE_PK_MAP = Variable.get("tables_to_load", deserialize_json=True)

    MINIO_CLIENT = Minio(
        'minio:9000',
        access_key=os.getenv('MINIO_ROOT_USER', 'minio'),
        secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'minio123'),
        secure=False,
    )

    # Ensure the bucket used in the pipeline exists
    BUCKET_NAME = 'extracted-data'
    if not MINIO_CLIENT.bucket_exists(BUCKET_NAME):
        MINIO_CLIENT.make_bucket(BUCKET_NAME)

    # Define JSON columns per table (based on schema)
    JSON_COLUMNS = {
        'aircrafts_data': ['model'],
        'airports_data': ['city'],  # Assuming airports_data has a JSON column 'city'
        # Add other tables with JSON columns if applicable
    }

    # Extract Task Group: Extract data from source DB and save to MinIO
    def extract_table(table_name, **kwargs):
        try:
            logger.info(f"Extracting table: {table_name}")
            # Connect via Airflow connection
            hook = PostgresHook(postgres_conn_id="sources-conn")
            conn = hook.get_conn()

            # Build query (incremental or full)
            if incremental:
                ds = kwargs["ds"]  # e.g. 2025-06-13
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

            # Skip if no new data
            if df.empty:
                raise AirflowSkipException(f"No new data for {table_name}")

            # Handle JSON columns: Convert to string for CSV storage
            json_cols = JSON_COLUMNS.get(table_name, [])
            for col in json_cols:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)

            # Save to CSV
            csv_path = f"/tmp/{table_name}.csv"
            df.to_csv(csv_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

            # Upload to MinIO
            object_name = f"temp/{table_name}.csv"
            MINIO_CLIENT.fput_object(BUCKET_NAME, object_name, csv_path)
            logger.info(f"Uploaded {table_name}.csv to MinIO at {object_name}")

            # Clean up
            os.remove(csv_path)
        except Exception as e:
            logger.error(f"Error extracting table {table_name}: {str(e)}")
            raise

    with TaskGroup(group_id='extract', tooltip='Extract data from source to MinIO') as extract_group:
        extract_tasks = []
        for table_name in TABLES:
            task = PythonOperator(
                task_id=f'extract_{table_name}',
                python_callable=extract_table,
                op_kwargs={'table_name': table_name},
            )
            extract_tasks.append(task)

    # Load Task Group: Load data from MinIO to staging schema
    def load_table(table_name, **kwargs):
        try:
            logger.info(f"Loading table: {table_name}")
            # Download from MinIO
            object_name = f"temp/{table_name}.csv"
            csv_path = f"/tmp/{table_name}.csv"
            MINIO_CLIENT.fget_object(BUCKET_NAME, object_name, csv_path)

            # Read CSV
            df = pd.read_csv(csv_path, keep_default_na=False, na_values=['NaN', ''])

            # Connect via Airflow connection
            hook = PostgresHook(postgres_conn_id="warehouse-conn")
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Clear / upsert logic
            if incremental:
                ds = kwargs["ds"]
                until = f"{ds} 23:59:59"
                # Remove rows from the same date window to be re-inserted
                cursor.execute(
                    f"DELETE FROM stg.{table_name} WHERE updated_at >= %s AND updated_at <= %s",
                    (ds, until),
                )
            else:
                cursor.execute(f"DELETE FROM stg.{table_name}")

            # Prepare data for insertion
            columns = df.columns.tolist()
            json_cols = JSON_COLUMNS.get(table_name, [])

            for col in json_cols:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)

            # Convert all other columns to tuples, handling NULL values
            values = [
                tuple(None if pd.isna(val) else val for val in row)
                for row in df.values
            ]

            # Insert data
            insert_query = f"INSERT INTO stg.{table_name} ({', '.join(columns)}) VALUES %s"
            execute_values(cursor, insert_query, values)

            # Commit and close
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Successfully loaded {table_name} into staging schema")

            # Clean up
            os.remove(csv_path)
        except Exception as e:
            logger.error(f"Error loading table {table_name}: {str(e)}")
            raise

    with TaskGroup(group_id='load', tooltip='Load data from MinIO to staging') as load_group:
        load_tasks = []
        for table_name in TABLES:
            task = PythonOperator(
                task_id=f'load_{table_name}',
                python_callable=load_table,
                op_kwargs={'table_name': table_name},
                trigger_rule='all_success',  # skip if extract skipped
            )
            load_tasks.append(task)

            # set dependency extract -> load for same table
            dag.get_task(f'extract_{table_name}') >> task

        # Sequential dependencies in specified order
        for upstream, downstream in zip(load_tasks, load_tasks[1:]):
            upstream >> downstream

    # Transform Task Group: Transform staging data into warehouse schema
    with TaskGroup(group_id='transform', tooltip='Transform staging data into warehouse') as transform_group:
        transform_order = [
            ('dim_aircrafts', 'dim_aircrafts.sql'),
            ('dim_airport', 'dim_airport.sql'),
            ('dim_passenger', 'dim_passenger.sql'),
            ('dim_seat', 'dim_seat.sql'),
            ('fct_boarding_pass', 'fct_boarding_pass.sql'),
            ('fct_booking_ticket', 'fct_booking_ticket.sql'),
            ('fct_flight_activity', 'fct_flight_activity.sql'),
            ('fct_seat_occupied_daily', 'fct_seat_occupied_daily.sql'),
        ]

        transform_tasks = []
        for task_name, sql_file in transform_order:
            t = PostgresOperator(
                task_id=f'transform_{task_name}',
                postgres_conn_id='warehouse-conn',
                sql=f'/opt/airflow/include/transformations/{sql_file}',
            )
            transform_tasks.append(t)

        # chain sequentially in order defined
        for up, down in zip(transform_tasks, transform_tasks[1:]):
            up >> down

    # Set dependencies between Task Groups
    extract_group >> load_group >> transform_group