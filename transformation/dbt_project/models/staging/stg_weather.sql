-- Staging: casi 1:1 con Silver, sin lógica de negocio todavía. Acá solo se
-- fija qué columnas de Silver le importan al resto del proyecto y en qué
-- orden/nombre — si mañana Silver agrega una columna nueva que no nos
-- interesa, este modelo actúa de filtro, y los modelos de más arriba
-- (intermediate, gold) nunca ven ese cambio.

WITH source AS (

    SELECT * FROM {{ source('silver', 'silver_weather') }}

)

SELECT
    event_time,
    city,
    country,
    temperature_celsius,
    humidity_percentage,
    wind_speed_m_s,
    condition

FROM source
