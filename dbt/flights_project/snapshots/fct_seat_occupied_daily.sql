{% snapshot fct_seat_occupied_daily %}

{{
    config(
      target_database='postgres',
      target_schema='final',
      unique_key=('date_flight', 'flight_nk'),
      strategy='check',
      check_cols=[
        'flight_no',
        'departure_airport',
        'arrival_airport',
        'aircraft_code',
        'status',
        'total_seat',
        'seat_occupied',
        'empty_seats'
      ]
    )
}}

WITH 
    stg_flights AS (
        SELECT * FROM {{ source('staging', 'flights') }}
    ),
    dim_dates AS (
        SELECT * FROM {{ ref('dim_date') }}
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
    stg_seats AS (
        SELECT * FROM {{ source('staging', 'seats') }}
    ),
    
    cnt_seat_occupied AS (
        SELECT
            sf.flight_id,
            COUNT(seat_no) AS seat_occupied
        FROM {{ source('staging', 'flights') }} sf
        JOIN {{ source('staging', 'boarding_passes') }} sbp 
            ON sbp.flight_id = sf.flight_id
        WHERE sf.status = 'Arrived'
        GROUP BY 1
    ),
    
    cnt_total_seats AS (
        SELECT
            aircraft_code,
            COUNT(seat_no) AS total_seat
        FROM {{ source('staging', 'seats') }}
        GROUP BY 1
    ),
    
    final_fct_seat_occupied_daily AS (
        SELECT 
            dd.date_id AS date_flight,
            sf.flight_id AS flight_nk,
            sf.flight_no,
            da1.airport_id AS departure_airport,
            da2.airport_id AS arrival_airport,
            dac.aircraft_id AS aircraft_code,
            sf.status,
            cts.total_seat,
            cso.seat_occupied,
            (cts.total_seat - cso.seat_occupied) AS empty_seats
        FROM {{ source('staging', 'flights') }} sf
        JOIN {{ ref('dim_date') }} dd ON dd.date_actual = DATE(sf.actual_departure)
        JOIN {{ ref('dim_airport') }} da1 ON da1.airport_nk = sf.departure_airport
        JOIN {{ ref('dim_airport') }} da2 ON da2.airport_nk = sf.arrival_airport
        JOIN {{ ref('dim_aircrafts') }} dac ON dac.aircraft_nk = sf.aircraft_code
        JOIN cnt_seat_occupied cso ON cso.flight_id = sf.flight_id
        JOIN cnt_total_seats cts ON cts.aircraft_code = sf.aircraft_code
    )

SELECT * FROM final_fct_seat_occupied_daily

{% endsnapshot %}