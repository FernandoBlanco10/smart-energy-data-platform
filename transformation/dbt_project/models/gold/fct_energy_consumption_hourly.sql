-- Grano: una fila por hora real de consumo. Esta es una tabla de Gold —
-- la va a consultar directo un dashboard o el agente NL→SQL, por eso se
-- materializa como tabla física (no vista, ver dbt_project.yml) con
-- nombre y columnas estables.
--
-- grid_region se queda en la tabla aunque hoy solo tiene un valor posible
-- ("PJME") — es una "dimensión degenerada": no amerita su propia tabla
-- dim_ porque no tiene atributos descriptivos propios más allá del
-- nombre en sí, pero sirve para filtrar/agrupar si algún día se suma
-- otra región de red eléctrica.

SELECT
    event_time,
    grid_region,
    consumption_mw,
    prev_hour_consumption_mw,
    next_hour_consumption_mw,
    rolling_24h_avg_consumption_mw,
    consumption_mw - prev_hour_consumption_mw AS hour_over_hour_change_mw

FROM {{ ref('int_energy_hourly') }}
