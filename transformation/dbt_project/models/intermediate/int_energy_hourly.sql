-- Cada fila ya es una hora real de consumo (energy es horario por diseño).
-- LAG/LEAD miran la hora anterior/siguiente de la MISMA región para medir
-- variación hora a hora; el promedio móvil de 24 horas suaviza el ruido y
-- muestra la tendencia diaria sin picos puntuales.
--
-- hour_of_day existe para poder compararse con weather más adelante
-- (int_hourly_climate_demand_pattern.sql) — no se puede unir por fecha
-- exacta: energy es el histórico PJME 2002-2018 reproducido como stream,
-- weather es en vivo con fecha de hoy. Nunca van a coincidir en fecha
-- calendario, así que se compara por hora del día en su lugar (ver
-- build-log.md, 2026-08-11).

WITH energy AS (

    SELECT * FROM {{ ref('stg_energy') }}

)

SELECT
    event_time,
    grid_region,
    consumption_mw,
    HOUR(event_time) AS hour_of_day,

    LAG(consumption_mw) OVER (
        PARTITION BY grid_region ORDER BY event_time
    ) AS prev_hour_consumption_mw,

    LEAD(consumption_mw) OVER (
        PARTITION BY grid_region ORDER BY event_time
    ) AS next_hour_consumption_mw,

    AVG(consumption_mw) OVER (
        PARTITION BY grid_region
        ORDER BY event_time
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) AS rolling_24h_avg_consumption_mw

FROM energy
