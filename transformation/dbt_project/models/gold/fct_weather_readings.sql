-- Grano: una fila por lectura de clima por ciudad. Acá sí se une con una
-- dimensión de verdad (dim_city, un seed — ver seeds/dim_city.csv) porque
-- aporta un atributo que Silver nunca tuvo: el estado (state) de cada
-- ciudad. Eso no se puede derivar de los datos que llegan por Kafka, hay
-- que declararlo a mano en algún lado — es la diferencia real entre una
-- dimensión de verdad y una "degenerada" como grid_region en
-- fct_energy_consumption_hourly.sql: acá SÍ hay un atributo nuevo que
-- agregar, no es solo repetir la misma clave.

SELECT
    w.event_time,
    w.city,
    dc.state,
    w.country,
    dc.region,
    w.temperature_celsius,
    w.humidity_percentage,
    w.wind_speed_m_s,
    w.condition,
    w.hour_of_day,
    w.prev_reading_temperature_celsius,
    w.next_reading_temperature_celsius,
    w.rolling_avg_temperature_celsius

FROM {{ ref('int_weather_readings') }} w
LEFT JOIN {{ ref('dim_city') }} dc
    ON w.city = dc.city
