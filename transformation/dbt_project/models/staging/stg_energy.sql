-- Staging: casi 1:1 con Silver. Ver stg_weather.sql para la justificación
-- completa de por qué existe esta capa aunque no transforme casi nada.

WITH source AS (

    SELECT * FROM {{ source('silver', 'silver_energy') }}

)

SELECT
    event_time,
    grid_region,
    consumption_mw

FROM source
