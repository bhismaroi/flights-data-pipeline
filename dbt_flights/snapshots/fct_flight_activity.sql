{% snapshot fct_flight_activity %}

{{
    config(
      target_database='postgres',
      target_schema='final',
      unique_key='flight_nk',
      strategy='check',
      check_cols=[
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
        'actual_departure_date_local',
        'actual_departure_date_utc',
        'actual_departure_time_local',
        'actual_departure_time_utc',
        'actual_arrival_date_local',
        'actual_arrival_date_utc',
        'actual_arrival_time_local',
        'actual_arrival_time_utc',
        'status',
        'delay_departure',
        'delay_arrival',
        'travel_time'
      ]
    )
}}

WITH 
    stg_flights AS (
        SELECT * FROM {{ source("staging", "flights") }}
    ),
    dim_times AS (
        SELECT * FROM {{ ref("dim_time") }}
    ),
    dim_dates AS (
        SELECT * FROM {{ ref("dim_date") }}
    ),
    dim_airports AS (
        SELECT * FROM {{ ref("dim_airport") }}
    ),
    dim_aircrafts AS (
        SELECT * FROM {{ ref("dim_aircrafts") }}
    ),
    final_fct_flight_activities AS (
        SELECT 
            sf.flight_id AS flight_nk,
            sf.flight_no,
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
            dd5.date_id AS actual_departure_date_local,
            dd6.date_id AS actual_departure_date_utc,
            dt5.time_id AS actual_departure_time_local,
            dt6.time_id AS actual_departure_time_utc,
            dd7.date_id AS actual_arrival_date_local,
            dd8.date_id AS actual_arrival_date_utc,
            dt7.time_id AS actual_arrival_time_local,
            dt8.time_id AS actual_arrival_time_utc,
            sf.status,
            (sf.actual_departure - sf.scheduled_departure) AS delay_departure,
            (sf.actual_arrival - sf.scheduled_arrival) AS delay_arrival,
            (sf.actual_arrival - sf.actual_departure) AS travel_time
        FROM stg_flights sf
        JOIN dim_dates dd1 ON dd1.date_actual = DATE(sf.scheduled_departure)
        JOIN dim_dates dd2 ON dd2.date_actual = DATE(sf.scheduled_departure AT TIME ZONE 'UTC')
        JOIN dim_times dt1 ON dt1.time_actual::time = sf.scheduled_departure::time
        JOIN dim_times dt2 ON dt2.time_actual::time = (sf.scheduled_departure AT TIME ZONE 'UTC')::time
        JOIN dim_dates dd3 ON dd3.date_actual = DATE(sf.scheduled_arrival)
        JOIN dim_dates dd4 ON dd4.date_actual = DATE(sf.scheduled_arrival AT TIME ZONE 'UTC')
        JOIN dim_times dt3 ON dt3.time_actual::time = sf.scheduled_arrival::time
        JOIN dim_times dt4 ON dt4.time_actual::time = (sf.scheduled_arrival AT TIME ZONE 'UTC')::time
        JOIN dim_airports da1 ON da1.airport_nk = sf.departure_airport
        JOIN dim_airports da2 ON da2.airport_nk = sf.arrival_airport
        JOIN dim_aircrafts dac ON dac.aircraft_nk = sf.aircraft_code
        JOIN dim_dates dd5 ON dd5.date_actual = DATE(sf.actual_departure)
        JOIN dim_dates dd6 ON dd6.date_actual = DATE(sf.actual_departure AT TIME ZONE 'UTC')
        JOIN dim_times dt5 ON dt5.time_actual::time = sf.actual_departure::time
        JOIN dim_times dt6 ON dt6.time_actual::time = (sf.actual_departure AT TIME ZONE 'UTC')::time
        JOIN dim_dates dd7 ON dd7.date_actual = DATE(sf.actual_arrival)
        JOIN dim_dates dd8 ON dd8.date_actual = DATE(sf.actual_arrival AT TIME ZONE 'UTC')
        JOIN dim_times dt7 ON dt7.time_actual::time = sf.actual_arrival::time
        JOIN dim_times dt8 ON dt8.time_actual::time = (sf.actual_arrival AT TIME ZONE 'UTC')::time
    )

SELECT * FROM final_fct_flight_activities

{% endsnapshot %}
