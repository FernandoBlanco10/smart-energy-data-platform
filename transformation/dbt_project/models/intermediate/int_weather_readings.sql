-- A diferencia de energy, weather no llega en una grilla horaria perfecta
-- — llega cada ~2 minutos, mientras corre weather_producer.py. LAG/LEAD acá
-- miran la lectura anterior/siguiente de la MISMA ciudad (PARTITION BY
-- city), no "la hora anterior" literal. El promedio móvil usa una ventana
-- de 5 lecturas (~10 minutos) en vez de 24 horas, por la misma razón:
-- no tiene sentido un promedio "diario" sobre algo que no llega por hora.

WITH weather AS (

    SELECT * FROM {{ ref('stg_weather') }}

)

SELECT
    event_time,
    city,
    country,
    temperature_celsius,
    humidity_percentage,
    wind_speed_m_s,
    condition,
    HOUR(event_time) AS hour_of_day,

    LAG(temperature_celsius) OVER (
        PARTITION BY city ORDER BY event_time
    ) AS prev_reading_temperature_celsius,

    LEAD(temperature_celsius) OVER (
        PARTITION BY city ORDER BY event_time
    ) AS next_reading_temperature_celsius,

    AVG(temperature_celsius) OVER (
        PARTITION BY city
        ORDER BY event_time
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_temperature_celsius

FROM weather
