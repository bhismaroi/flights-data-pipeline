{% snapshot dim_seat %}

{{
    config(
      target_database='postgres',  
      target_schema='final',
      unique_key='seat_id',
      strategy='check',
      check_cols=[
        'aircraft_id',
        'seat_no',
        'fare_conditions'
      ]
    )
}}

WITH stg_dim_seats AS (
    SELECT
        s.id AS seat_id,
        da.aircraft_id AS aircraft_id,
        s.seat_no,
        s.fare_conditions
    FROM {{ source("staging", "seats") }} s
    JOIN {{ ref("dim_aircrafts") }} da
        ON da.aircraft_nk = s.aircraft_code
)

SELECT
    seat_id,
    aircraft_id,
    seat_no,
    fare_conditions
FROM stg_dim_seats

{% endsnapshot %}