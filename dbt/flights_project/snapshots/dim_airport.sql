{% snapshot dim_airport %}

{{
    config(
      target_database='postgres',  
      target_schema='final',
      unique_key='airport_id',
      strategy='check',
      check_cols=[
        'airport_nk',
        'airport_name',
        'city',
        'coordinates',
        'timezone'
      ]
    )
}}

SELECT
    ad.id AS airport_id,
    ad.airport_code AS airport_nk,
    ad.airport_name,
    ad.city->>'en' AS city,
    ad.coordinates,
    ad.timezone
FROM {{ source("staging", "airports_data") }} ad

{% endsnapshot %}
