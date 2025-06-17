{% snapshot fct_booking_ticket %}

{{
    config(
      target_database='postgres',
      target_schema='final',
      unique_key=('book_nk', 'ticket_no', 'flight_nk'),
      strategy='check',
      check_cols=[
        'book_date_local',
        'book_date_utc',
        'book_time_local',
        'book_time_utc',
        'total_amount',
        'passenger_id',
        'fare_conditions',
        'amount',
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
        'status',
        'aircraft_code',
        'actual_departure_date_local',
        'actual_departure_date_utc',
        'actual_departure_time_local',
        'actual_departure_time_utc',
        'actual_arrival_date_local',
        'actual_arrival_date_utc',
        'actual_arrival_time_local',
        'actual_arrival_time_utc'
      ]
    )
}}

WITH stg_fct_booking_ticket AS (

    SELECT
        sb.book_ref AS book_nk,
        dd1.date_id AS book_date_local,
        dd2.date_id AS book_date_utc,
        dt1.time_id AS book_time_local,
        dt2.time_id AS book_time_utc,
        sb.total_amount AS total_amount,
        st.ticket_no,
        dp.passenger_id,
        stf.flight_id AS flight_nk,
        stf.fare_conditions,
        stf.amount,
        sf.flight_no,
        dd3.date_id AS scheduled_departure_date_local,
        dd4.date_id AS scheduled_departure_date_utc,
        dt3.time_id AS scheduled_departure_time_local,
        dt4.time_id AS scheduled_departure_time_utc,
        dd5.date_id AS scheduled_arrival_date_local,
        dd6.date_id AS scheduled_arrival_date_utc,
        dt5.time_id AS scheduled_arrival_time_local,
        dt6.time_id AS scheduled_arrival_time_utc,
        da1.airport_id AS departure_airport,
        da2.airport_id AS arrival_airport,
        sf.status,
        dac.aircraft_id AS aircraft_code,
        dd7.date_id AS actual_departure_date_local,
        dd8.date_id AS actual_departure_date_utc,
        dt7.time_id AS actual_departure_time_local,
        dt8.time_id AS actual_departure_time_utc,
        dd9.date_id AS actual_arrival_date_local,
        dd10.date_id AS actual_arrival_date_utc,
        dt9.time_id AS actual_arrival_time_local,
        dt10.time_id AS actual_arrival_time_utc
        
    FROM {{ source('staging', 'bookings') }} sb
    
    JOIN {{ ref('dim_date') }} dd1 ON dd1.date_actual = DATE(sb.book_date)
    JOIN {{ ref('dim_date') }} dd2 ON dd2.date_actual = DATE(sb.book_date AT TIME ZONE 'UTC')
    JOIN {{ ref('dim_time') }} dt1 ON dt1.time_actual::time = sb.book_date::time
    JOIN {{ ref('dim_time') }} dt2 ON dt2.time_actual::time = (sb.book_date AT TIME ZONE 'UTC')::time
    
    JOIN {{ source('staging', 'tickets') }} st ON st.book_ref = sb.book_ref
    JOIN {{ ref('dim_passenger') }} dp ON dp.passenger_nk = st.passenger_id
    JOIN {{ source('staging', 'ticket_flights') }} stf ON stf.ticket_no = st.ticket_no
    JOIN {{ source('staging', 'flights') }} sf ON sf.flight_id = stf.flight_id
    
    JOIN {{ ref('dim_date') }} dd3 ON dd3.date_actual = DATE(sf.scheduled_departure)
    JOIN {{ ref('dim_date') }} dd4 ON dd4.date_actual = DATE(sf.scheduled_departure AT TIME ZONE 'UTC')
    JOIN {{ ref('dim_time') }} dt3 ON dt3.time_actual::time = (sf.scheduled_departure)::time
    JOIN {{ ref('dim_time') }} dt4 ON dt4.time_actual::time = (sf.scheduled_departure AT TIME ZONE 'UTC')::time
    
    JOIN {{ ref('dim_date') }} dd5 ON dd5.date_actual = DATE(sf.scheduled_arrival)
    JOIN {{ ref('dim_date') }} dd6 ON dd6.date_actual = DATE(sf.scheduled_arrival AT TIME ZONE 'UTC')
    JOIN {{ ref('dim_time') }} dt5 ON dt5.time_actual::time = (sf.scheduled_arrival)::time
    JOIN {{ ref('dim_time') }} dt6 ON dt6.time_actual::time = (sf.scheduled_arrival AT TIME ZONE 'UTC')::time
    
    JOIN {{ ref('dim_airport') }} da1 ON da1.airport_nk = sf.departure_airport
    JOIN {{ ref('dim_airport') }} da2 ON da2.airport_nk = sf.arrival_airport
    JOIN {{ ref('dim_aircrafts') }} dac ON dac.aircraft_nk = sf.aircraft_code
    
    JOIN {{ ref('dim_date') }} dd7 ON dd7.date_actual = DATE(sf.actual_departure)
    JOIN {{ ref('dim_date') }} dd8 ON dd8.date_actual = DATE(sf.actual_departure AT TIME ZONE 'UTC')
    JOIN {{ ref('dim_time') }} dt7 ON dt7.time_actual::time = (sf.actual_departure)::time
    JOIN {{ ref('dim_time') }} dt8 ON dt8.time_actual::time = (sf.actual_departure AT TIME ZONE 'UTC')::time
    
    JOIN {{ ref('dim_date') }} dd9 ON dd9.date_actual = DATE(sf.actual_arrival)
    JOIN {{ ref('dim_date') }} dd10 ON dd10.date_actual = DATE(sf.actual_arrival AT TIME ZONE 'UTC')
    JOIN {{ ref('dim_time') }} dt9 ON dt9.time_actual::time = (sf.actual_arrival)::time
    JOIN {{ ref('dim_time') }} dt10 ON dt10.time_actual::time = (sf.actual_arrival AT TIME ZONE 'UTC')::time

)

SELECT * FROM stg_fct_booking_ticket

{% endsnapshot %}