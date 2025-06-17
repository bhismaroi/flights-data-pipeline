{% snapshot fct_boarding_pass %}

{{
    config(
      target_database='postgres',
      target_schema='final',
      unique_key=('ticket_no', 'flight_id', 'boarding_no'),
      strategy='check',
      check_cols=[
        'book_ref',
        'passenger_id',
        'flight_no',
        'scheduled_departure_date_local',
        'scheduled_departure_date_utc',
        'scheduled_departure_time_local',
        'scheduled_departure_time_utc',
        'scheduled_arrival_date_local',
        'scheduled_arrival_date_utc',
        'scheduled_arrival_time_local',
        'scheduled_arrival_time_utc',
        'departure_airport',
        'arrival_airport',
        'aircraft_code',
        'status',
        'fare_conditions',
        'seat_no'
      ]
    )
}}

WITH 
    stg_tickets AS (
        SELECT * FROM {{ source('staging', 'tickets') }}
    ),
    dim_passengers AS (
        SELECT * FROM {{ ref('dim_passenger') }}
    ),
    stg_ticket_flights AS (
        SELECT * FROM {{ source('staging', 'ticket_flights') }}
    ),
    stg_flights AS (
        SELECT * FROM {{ source('staging', 'flights') }}
    ),
    dim_dates AS (
        SELECT * FROM {{ ref('dim_date') }}
    ),
    dim_times AS (
        SELECT * FROM {{ ref('dim_time') }}
    ),
    dim_airports AS (
        SELECT * FROM {{ ref('dim_airport') }}
    ),
    dim_aircrafts AS (
        SELECT * FROM {{ ref('dim_aircrafts') }}
    ),
    stg_boarding_passes AS (
        SELECT * FROM {{ source('staging', 'boarding_passes') }}
    ),
    
    final_fct_boarding_pass AS (
        SELECT 
            st.ticket_no,
            st.book_ref,
            dp.passenger_id,
            stf.flight_id,
            sf.flight_no,
            sbp.boarding_no,
            dd1.date_id AS scheduled_departure_date_local,
            dd2.date_id AS scheduled_departure_date_utc,
            dt1.time_id AS scheduled_departure_time_local,
            dt2.time_id AS scheduled_departure_time_utc,
            dd3.date_id AS scheduled_arrival_date_local,
            dd4.date_id AS scheduled_arrival_date_utc,
            dt3.time_id AS scheduled_arrival_time_local,
            dt4.time_id AS scheduled_arrival_time_utc,
            da1.airport_id AS departure_airport,
            da2.airport_id AS arrival_airport,
            dac.aircraft_id AS aircraft_code,
            sf.status,
            stf.fare_conditions,
            sbp.seat_no
        FROM stg_tickets st
        JOIN dim_passengers dp ON dp.passenger_id = st.id
        JOIN stg_ticket_flights stf ON stf.ticket_no = st.ticket_no
        JOIN stg_flights sf ON sf.flight_id = stf.flight_id
        JOIN dim_dates dd1 ON dd1.date_actual = DATE(sf.scheduled_departure)
        JOIN dim_dates dd2 ON dd2.date_actual = DATE(sf.scheduled_departure AT TIME ZONE 'UTC')
        JOIN dim_times dt1 ON dt1.time_actual::time = (sf.scheduled_departure)::time
        JOIN dim_times dt2 ON dt2.time_actual::time = (sf.scheduled_departure AT TIME ZONE 'UTC')::time
        JOIN dim_dates dd3 ON dd3.date_actual = DATE(sf.scheduled_arrival)
        JOIN dim_dates dd4 ON dd4.date_actual = DATE(sf.scheduled_arrival AT TIME ZONE 'UTC')
        JOIN dim_times dt3 ON dt3.time_actual::time = (sf.scheduled_arrival)::time
        JOIN dim_times dt4 ON dt4.time_actual::time = (sf.scheduled_arrival AT TIME ZONE 'UTC')::time
        JOIN dim_airports da1 ON da1.airport_nk = sf.departure_airport
        JOIN dim_airports da2 ON da2.airport_nk = sf.arrival_airport
        JOIN dim_aircrafts dac ON dac.aircraft_nk = sf.aircraft_code
        JOIN stg_boarding_passes sbp ON sbp.flight_id = stf.flight_id AND sbp.ticket_no = stf.ticket_no
    )

SELECT * FROM final_fct_boarding_pass

{% endsnapshot %}