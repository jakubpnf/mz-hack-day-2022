{{ config(
    materialized ='materializedview'
) }}


SELECT fi.icao24,
       manufacturername,
       model,
       operator,
       origin_country,
       time_position,
       longitude,
       latitude,
       on_ground,
       velocity,
       baro_altitude,
       case when position('A380' in model) > 0 then 1
            when position('A318' in  model) > 0 then 0.2
           else 0.4 end as size,
       case when coalesce(baro_altitude,0) > 12000 then 1
           else coalesce(baro_altitude,0) / 12000 end as color,
       case when on_ground = 'f' and origin_country = 'United States' then 3
            when on_ground = 'f' and origin_country = 'China' then 5
            when on_ground = 'f' then 4
           else 2 end as exit_node
FROM {{ ref('stg_flight_information') }} fi
JOIN {{ ref('stg_icao_mapping') }} icao
ON fi.icao24 = icao.icao24
