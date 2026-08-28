-- El "join clima+consumo" real de esta fase — pero no por timestamp exacto.
-- energy es el histórico PJME 2002-2018 reproducido como stream; weather es
-- en vivo, con fecha de hoy. Nunca van a coincidir en fecha calendario, así
-- que en vez de unir evento a evento, se compara el patrón TÍPICO de cada
-- hora del día (0-23): ¿las horas más calurosas del día tienden a ser
-- también las de mayor consumo, sin importar en qué año haya ocurrido cada
-- cosa? Es una simplificación intencional del portafolio, documentada en
-- build-log.md (2026-08-11) — no una limitación oculta.
--
-- FULL OUTER JOIN a propósito: mientras weather_producer.py no lleve
-- corriendo 24 horas reales todavía, va a cubrir pocas horas del día. Un
-- INNER JOIN escondería ese hecho (mostraría menos filas sin explicar por
-- qué); el FULL OUTER JOIN + los conteos (n_energy_readings/
-- n_weather_readings) lo dejan visible.

WITH energy_by_hour AS (

    SELECT
        hour_of_day,
        AVG(consumption_mw) AS avg_consumption_mw,
        COUNT(*) AS n_energy_readings
    FROM {{ ref('int_energy_hourly') }}
    GROUP BY hour_of_day

),

weather_by_hour AS (

    SELECT
        hour_of_day,
        AVG(temperature_celsius) AS avg_temperature_celsius,
        COUNT(*) AS n_weather_readings
    FROM {{ ref('int_weather_readings') }}
    GROUP BY hour_of_day

)

SELECT
    COALESCE(e.hour_of_day, w.hour_of_day) AS hour_of_day,
    e.avg_consumption_mw,
    e.n_energy_readings,
    w.avg_temperature_celsius,
    w.n_weather_readings

FROM energy_by_hour e
FULL OUTER JOIN weather_by_hour w
    ON e.hour_of_day = w.hour_of_day
ORDER BY hour_of_day
