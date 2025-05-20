Flights Data Pipeline
Project Overview
This project implements an automated data pipeline for a simulated flight booking system as part of the Pacmann Job Preparation Program. The pipeline extracts data from a PostgreSQL source database (bookings schema), stores it as CSV files in a MinIO object store, loads it into a PostgreSQL staging schema, and transforms it into dimension and fact tables in a data warehouse. The entire workflow is orchestrated using Apache Airflow (version 2.10.2) with a DAG named flights_data_pipeline.
Architecture Description
The pipeline consists of the following components:

Data Source: PostgreSQL database (bookings schema) containing tables: aircrafts_data, airports_data, bookings, tickets, seats, flights, ticket_flights, and boarding_passes.
Data Lake: MinIO object store with a bucket named extracted-data for storing CSV files.
Data Warehouse: PostgreSQL database with staging schema (for raw data) and warehouse schema (for dimension and fact tables).
Orchestrator: Apache Airflow 2.10.2 in standalone mode, running the flights_data_pipeline DAG.
Setup Tool: Docker Compose to manage all services.

![!\[alt text\](image.png)](images/diagram.png)

Pipeline Flow
The flights_data_pipeline DAG is structured into three TaskGroups:

Extract:
Goal: Extract data from the bookings schema tables in the source PostgreSQL database.
Method: Uses PythonOperator to query each table and save the results as CSV files in MinIO (/extracted-data/temp/<table_name>.csv).
Tables: aircrafts_data, airports_data, bookings, tickets, seats, flights, ticket_flights, boarding_passes.
Execution: Tasks run in parallel.


Load:
Goal: Load CSV files from MinIO into the staging schema in the warehouse PostgreSQL database.
Method: Uses PythonOperator to read CSVs and upsert data into corresponding staging tables.
Sequence: Tasks run sequentially in the order: aircrafts_data → airports_data → bookings → tickets → seats → flights → ticket_flights → boarding_passes.


Transform:
Goal: Transform data from the staging schema into dimension and fact tables in the warehouse schema.
Method: Uses PostgresOperator to execute provided SQL scripts.
Output Tables: Dimension tables (dim_airport, dim_seat, dim_passenger) and fact tables (fct_booking_ticket, fct_flight_activity, etc.).
Execution: Tasks run sequentially, with dimension tables created before fact tables to respect dependencies.



How to Run and Simulate
Follow these steps to set up and run the project:

Clone the Repository:git clone <repository-url>
cd mentoring1


Set Up Environment Variables:
Copy .env.example to .env:cp .env.example .env


Generate a Fernet key for Airflow:python fernet.py


Update AIRFLOW_FERNET_KEY in .env with the generated key.


Place SQL Files:
Ensure source_init.sql, staging_schema.sql, and warehouse_init.sql are in include/.
Place transformation SQL scripts (e.g., dim_airport.sql, fct_booking_ticket.sql) in include/transformations/.


Start Services:docker-compose up -d


Configure Airflow:
Access Airflow UI at http://localhost:8080 (username: admin, password: admin).
Add a PostgreSQL connection for warehouse_db:
Conn Id: warehouse_db
Conn Type: Postgres
Host: warehouse_db
Schema: warehouse
Login: postgres
Password: postgres
Port: 5432




Deploy and Run the DAG:
Place flights_data_pipeline.py in dags/.
In Airflow UI, enable and trigger the flights_data_pipeline DAG.


Verify Data Flow:
MinIO: Check http://localhost:9001 for CSV files in extracted-data/temp/.
Staging Schema: Use psql to query staging tables:docker exec -it warehouse_db psql -U postgres -d warehouse -c "\dt staging.*"


Warehouse Schema: Verify dimension and fact tables:docker exec -it warehouse_db psql -U postgres -d warehouse -c "\dt warehouse.*"




Monitor Logs:
Check Airflow logs for errors:docker logs airflow_standalone





Screenshots

Airflow DAG Graph: Shows the task structure of flights_data_pipeline (extract, load, transform).
![!\[alt text\](image.png)](images/diagram.png)

MinIO Bucket: Displays CSV files in extracted-data/temp/.
![!\[alt text\](miniobucket.png)](images/miniobucket.png)

Warehouse Tables: Shows psql output of warehouse schema tables and sample data.
![!\[alt text\](warehousestg.png)](images/warehousestg.png)




Troubleshooting

DAG Not Visible: Check flights_data_pipeline.py for syntax errors and ensure it’s in dags/.
Task Failures:
Extract: Verify source_db connection and table existence.
Load: Ensure CSVs exist in MinIO and staging tables match source schema.
Transform: Confirm warehouse_db connection and SQL script validity.


Port Conflicts: Adjust ports in docker-compose.yml if needed (e.g., 5433:5432 to 5435:5432).

Submission
The project is submitted via a GitHub repository containing:

All project files (dags/, include/, Dockerfile, docker-compose.yml, etc.).
This README.md with complete documentation.
Screenshots demonstrating successful execution.

