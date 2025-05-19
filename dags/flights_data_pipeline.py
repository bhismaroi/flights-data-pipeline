# dags/flights_data_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
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

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG
with DAG(
    dag_id='flights_data_pipeline',
    default_args=default_args,
    description='A data pipeline for flight booking system',
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 19),
    catchup=False,
) as dag:

    # Configuration (using environment variables from .env)
    SOURCE_DB_CONN = {
        'host': 'source_db',
        'port': '5432',
        'dbname': os.getenv('SOURCE_DB_NAME', 'bookings'),
        'user': os.getenv('SOURCE_DB_USER', 'postgres'),
        'password': os.getenv('SOURCE_DB_PASSWORD', 'postgres'),
    }

    WAREHOUSE_DB_CONN = {
        'host': 'warehouse_db',
        'port': '5432',
        'dbname': os.getenv('WAREHOUSE_DB_NAME', 'warehouse'),
        'user': os.getenv('WAREHOUSE_DB_USER', 'postgres'),
        'password': os.getenv('WAREHOUSE_DB_PASSWORD', 'postgres'),
    }

    MINIO_CLIENT = Minio(
        'minio:9000',
        access_key=os.getenv('MINIO_ROOT_USER', 'minio'),
        secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'minio123'),
        secure=False,
    )

    TABLES = [
        'aircrafts_data',
        'airports_data',
        'bookings',
        'tickets',
        'seats',
        'flights',
        'ticket_flights',
        'boarding_passes',
    ]

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
            # Connect to source database
            conn = psycopg2.connect(**SOURCE_DB_CONN)
            query = f"SELECT * FROM bookings.{table_name}"
            df = pd.read_sql(query, conn)
            conn.close()

            # Handle JSON columns: Convert to string for CSV storage
            json_cols = JSON_COLUMNS.get(table_name, [])
            for col in json_cols:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)

            # Save to CSV
            csv_path = f"/tmp/{table_name}.csv"
            df.to_csv(csv_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

            # Upload to MinIO
            bucket_name = 'extracted-data'
            object_name = f"temp/{table_name}.csv"
            MINIO_CLIENT.fput_object(bucket_name, object_name, csv_path)
            logger.info(f"Uploaded {table_name}.csv to MinIO at {object_name}")

            # Clean up
            os.remove(csv_path)
        except Exception as e:
            logger.error(f"Error extracting table {table_name}: {str(e)}")
            raise

    with TaskGroup(group_id='extract', tooltip='Extract data from source to MinIO') as extract_group:
        extract_tasks = [
            PythonOperator(
                task_id=f'extract_{table_name}',
                python_callable=extract_table,
                op_kwargs={'table_name': table_name},
            ) for table_name in TABLES
        ]

    # Load Task Group: Load data from MinIO to staging schema
    def load_table(table_name, **kwargs):
        try:
            logger.info(f"Loading table: {table_name}")
            # Download from MinIO
            bucket_name = 'extracted-data'
            object_name = f"temp/{table_name}.csv"
            csv_path = f"/tmp/{table_name}.csv"
            MINIO_CLIENT.fget_object(bucket_name, object_name, csv_path)

            # Read CSV
            df = pd.read_csv(csv_path, keep_default_na=False, na_values=['NaN', ''])

            # Connect to warehouse database
            conn = psycopg2.connect(**WAREHOUSE_DB_CONN)
            cursor = conn.cursor()

            # Clear existing data (use DELETE to avoid foreign key issues)
            cursor.execute(f"DELETE FROM stg.{table_name}")

            # Prepare data for insertion
            columns = df.columns.tolist()
            json_cols = JSON_COLUMNS.get(table_name, [])
            
            for col in json_cols:
                if col in df.columns:
                    def escape_json(x):
                        if pd.notnull(x) and x != 'None':
                            return "'{}'::json".format(str(x).replace("'", "''"))
                        else:
                            return 'null'
                    df[col] = df[col].apply(escape_json)
                    df[col] = df[col].apply(lambda x: "'{}'::json".format(str(x).replace("'", "''")) if pd.notnull(x) and x != 'None' else 'null')

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
            )
            load_tasks.append(task)

        # Set sequential dependencies for load tasks (respects foreign keys)
        for i in range(len(load_tasks) - 1):
            load_tasks[i] >> load_tasks[i + 1]

    # Transform Task Group: Transform staging data into warehouse schema
    with TaskGroup(group_id='transform', tooltip='Transform staging data into warehouse') as transform_group:
        # Dimension tables
        dim_aircraft = PostgresOperator(
            task_id='transform_dim_aircraft',
            postgres_conn_id='warehouse_db',
            sql='/opt/airflow/include/transformations/dim_aircraft.sql',
        )

        dim_airport = PostgresOperator(
            task_id='transform_dim_airport',
            postgres_conn_id='warehouse_db',
            sql='/opt/airflow/include/transformations/dim_airport.sql',
        )

        dim_passenger = PostgresOperator(
            task_id='transform_dim_passenger',
            postgres_conn_id='warehouse_db',
            sql='/opt/airflow/include/transformations/dim_passenger.sql',
        )

        dim_seat = PostgresOperator(
            task_id='transform_dim_seat',
            postgres_conn_id='warehouse_db',
            sql='/opt/airflow/include/transformations/dim_seat.sql',
        )

        # Fact tables
        fct_boarding_pass = PostgresOperator(
            task_id='transform_fct_boarding_pass',
            postgres_conn_id='warehouse_db',
            sql='/opt/airflow/include/transformations/fct_boarding_pass.sql',
        )

        fct_booking_ticket = PostgresOperator(
            task_id='transform_fct_booking_ticket',
            postgres_conn_id='warehouse_db',
            sql='/opt/airflow/include/transformations/fct_booking_ticket.sql',
        )

        fct_flight_activity = PostgresOperator(
            task_id='transform_fct_flight_activity',
            postgres_conn_id='warehouse_db',
            sql='/opt/airflow/include/transformations/fct_flight_activity.sql',
        )

        fct_seat_occupied_daily = PostgresOperator(
            task_id='transform_fct_seat_occupied_daily',
            postgres_conn_id='warehouse_db',
            sql='/opt/airflow/include/transformations/fct_seat_occupied_daily.sql',
        )

        # Define dependencies for transformations
        [dim_aircraft, dim_airport, dim_passenger] >> dim_seat
        dim_seat >> [fct_boarding_pass, fct_booking_ticket, fct_flight_activity, fct_seat_occupied_daily]

    # Set dependencies between Task Groups
    extract_group >> load_group >> transform_group