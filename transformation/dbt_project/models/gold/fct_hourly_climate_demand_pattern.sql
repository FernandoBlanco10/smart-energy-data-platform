-- Graduación directa desde intermediate: toda la lógica de combinar
-- clima+consumo ya está resuelta en int_hourly_climate_demand_pattern.sql
-- (ver build-log.md 2026-08-11 para el porqué del join por hora del día
-- en vez de por fecha exacta). Acá el único trabajo es materializarla
-- como tabla física estable, con nombre de Gold, lista para que el
-- dashboard o el agente NL→SQL la consulten sin tener que saber nada de
-- cómo se construyó por dentro.

SELECT * FROM {{ ref('int_hourly_climate_demand_pattern') }}
