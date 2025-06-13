{% snapshot dim_passenger %}

{{
    config(
      target_database='postgres', 
      target_schema='final',
      unique_key='passenger_id',
      strategy='check',
      check_cols=[
        'passenger_nk',
        'passenger_name',
        'phone',
        'email'
      ]
    )
}}

SELECT
    t.id AS passenger_id,
    t.passenger_id AS passenger_nk,
    t.passenger_name,
    t.contact_data->>'phone' AS phone,
    t.contact_data->>'email' AS email
FROM {{ source("staging", "tickets") }} t

{% endsnapshot %}