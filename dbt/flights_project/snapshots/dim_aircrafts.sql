{% snapshot dim_aircrafts %}

{{
    config(
      target_database='postgres',
      target_schema='final',
      unique_key='aircraft_id',
      strategy='check',
      check_cols=[
        'aircraft_nk',
        'model',
        'range'
		]
    )
}}

SELECT
    ad.id AS aircraft_id,
    ad.aircraft_code AS aircraft_nk,
    ad.model,
    ad.range
FROM {{ source("staging","aircrafts_data") }} ad

{% endsnapshot %}