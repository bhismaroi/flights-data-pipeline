from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
from minio import Minio
import psycopg2
from psycopg2.extras import execute_values

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Configuration
SOURCE_DB_CONN = {
    'host': 'source_db',
    'port': '5432',
    'dbname': 'bookings',
    'user': 'postgres',
    'password': 'postgres'
}

WAREHOUSE_DB_CONN = {
    'host': 'warehouse_db',
    'port': '5432',
    'dbname': 'warehouse',
    'user': 'postgres',
    'password': 'postgres'
}

MINIO_CLIENT = {
    'endpoint': 'minio:9000',
    'access_key': 'minio',
    'secret_key': 'minio123',
    'secure': False
}

TABLES = [
    'aircrafts_data',
    'airports_data',
    'bookings',
    'tickets',
    'seats',
    'flights',
    'ticket_flights',
    'boarding_passes'
]

# Extraction function
def extract_table(table_name):
    # Connect to source database
    conn = psycopg2.connect(**SOURCE_DB_CONN)
    cursor = conn.cursor()
    
    # Extract data
    cursor.execute(f"SELECT * FROM {table_name}")
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    # Convert to DataFrame and save as CSV
    df = pd.DataFrame(data, columns=columns)
    csv_path = f"/tmp/{table_name}.csv"
    df.to_csv(csv_path, index=False)
    
    # Upload to MinIO
    minio_client = Minio(**MINIO_CLIENT)
    bucket_name = 'extracted-data'
    object_name = f"temp/{table_name}.csv"
    
    # Ensure bucket exists
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    
    minio_client.fput_object(bucket_name, object_name, csv_path)
    
    # Clean up
    cursor.close()
    conn.close()

# Load function
def load_table(table_name):
    # Initialize MinIO client
    minio_client = Minio(**MINIO_CLIENT)
    bucket_name = 'extracted-data'
    object_name = f"temp/{table_name}.csv"
    
    # Download CSV from MinIO
    csv_path = f"/tmp/{table_name}.csv"
    minio_client.fget_object(bucket_name, object_name, csv_path)
    
    # Read CSV
    df = pd.read_csv(csv_path)
    
    # Connect to warehouse database
    conn = psycopg2.connect(**WAREHOUSE_DB_CONN)
    cursor = conn.cursor()
    
    # Prepare upsert query (assumes staging tables have same structure as source)
    columns = df.columns.tolist()
    columns_str = ", ".join(columns)
    values_str = ", ".join([f"%s" for _ in columns])
    conflict_columns = columns  # Adjust based on primary key if known
    conflict_str = ", ".join(conflict_columns)
    update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])
    
    upsert_query = f"""
        INSERT INTO staging.{table_name} ({columns_str})
        VALUES ({values_str})
        ON CONFLICT ({conflict_str}) DO UPDATE
        SET {update_str}
    """
    
    # Upsert data
    values = [tuple(row) for row in df.values]
    execute_values(cursor, upsert_query, values)
    
    # Commit and clean up
    conn.commit()
    cursor.close()
    conn.close()

# Define DAG
with DAG(
    dag_id='flights_dataHumanline',
    default_args=default_args,
    description='Flights Data Pipeline for Pacmann Job Preparation Program',
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 16),
    catchup=False,
) as dag:

    # Extract TaskGroup
    with TaskGroup(group_id='extract') as extract_group:
        extract_tasks = [
            PythonOperator(
                task_id=f'extract_{table}',
                python_callable=extract_table,
                op_kwargs={'table_name': table}
            ) for table in TABLES
        ]

    # Load TaskGroup
    with TaskGroup(group_id='load') as load_group:
        load_tasks = []
        prev_task = None
        for table in TABLES:
            task = PythonOperator(
                task_id=f'load_{table}',
                python_callable=load_table,
                op_kwargs={'table_name': table}
            )
            load_tasks.append(task)
            if prev_task:
                prev_task >> task
            prev_task = task

    # Transform TaskGroup
    with TaskGroup(group_id='transform') as transform_group:
        # Dimension tables
        dim_tasks = [
            PostgresOperator(
                task_id=f'transform_dim_airport',
                postgres_conn_id='warehouse_db',
                sql='transformations/dim_airport.sql'
            ),
            PostgresOperator(
                task_id=f'transform_dim_seat',
                postgres_conn_id='warehouse_db',
                sql='transformations/dim_seat.sql'
            ),
            PostgresOperator(
                task_id=f'transform_dim_passenger',
                postgres_conn_id='warehouse_db',
                sql='transformations/dim_passenger.sql'
            )
        ]
        
        # Fact tables (assumed to depend on dimension tables)
        fact_tasks = [
            PostgresOperator(
                task_id=f'transform_fct_booking_ticket',
                postgres_conn_id='warehouse_db',
                sql='transformations/fct_booking_ticket.sql'
            ),
            PostgresOperator(
                task_id=f'transform_fct_flight_activity',
                postgres_conn_id='warehouse_db',
                sql='transformations/fct_flight_activity.sql'
            )
        ]
        
        # Set dependencies: dimension tables before fact tables
        for dim_task in dim_tasks:
            for fact_task in fact_tasks:
                dim_task >> fact_task

    # Set inter-TaskGroup dependencies
    extract_group >> load_group >> transform_group