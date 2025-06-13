#!/bin/bash

# Initialize Airflow metadata DB and user (if first run)
airflow db upgrade

# Create an admin user if it does not exist
airflow users create \
    --username "admin" \
    --firstname "Admin" \
    --lastname "User" \
    --role "Admin" \
    --email "admin@example.com" \
    --password "${AIRFLOW_ADMIN_PWD:-admin}" || true

# ------------------------------------------------------------------
# Variables
cat <<EOF >/tmp/airflow_variables.json
{
  "incremental": "True",
  "tables_to_extract": [
    "aircrafts_data", "airports_data", "bookings", "tickets", "seats",
    "flights", "ticket_flights", "boarding_passes"
  ],
  "tables_to_load": {
    "aircrafts_data": "aircraft_code",
    "airports_data": "airport_code",
    "bookings": "book_ref",
    "tickets": "ticket_no",
    "seats": ["aircraft_code", "seat_no"],
    "flights": "flight_id",
    "ticket_flights": ["ticket_no", "flight_id"],
    "boarding_passes": ["ticket_no", "flight_id"]
  }
}
EOF

airflow variables import /tmp/airflow_variables.json || true

# ------------------------------------------------------------------
# Connections
# Source database connection
airflow connections add sources-conn \
    --conn-type postgres \
    --conn-host "source_db" \
    --conn-login "${SOURCE_DB_USER:-postgres}" \
    --conn-password "${SOURCE_DB_PASSWORD:-postgres}" \
    --conn-schema "${SOURCE_DB_NAME:-bookings}" \
    --conn-port 5432 || true

# Warehouse database connection
airflow connections add warehouse-conn \
    --conn-type postgres \
    --conn-host "warehouse_db" \
    --conn-login "${WAREHOUSE_DB_USER:-postgres}" \
    --conn-password "${WAREHOUSE_DB_PASSWORD:-postgres}" \
    --conn-schema "${WAREHOUSE_DB_NAME:-warehouse}" \
    --conn-port 5432 || true

# MinIO connection (using aws-type, extra stores endpoint_url)
airflow connections add minio-conn \
    --conn-type aws \
    --conn-login "${MINIO_ROOT_USER:-minio}" \
    --conn-password "${MINIO_ROOT_PASSWORD:-minio123}" \
    --conn-extra '{"endpoint_url": "http://minio:9000"}' || true

# Slack notifier connection
airflow connections add slack_notifier \
    --conn-type http \
    --conn-host "https://hooks.slack.com" \
    --conn-extra '{"webhook_path": "..."}' || true

# ------------------------------------------------------------------
# Decide which Airflow component to start
cmd=${1:-webserver}

if [ "$cmd" = "scheduler" ]; then
    exec airflow scheduler
else
    exec airflow webserver -p 8080
fi