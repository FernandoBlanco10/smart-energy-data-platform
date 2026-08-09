"""
Reglas de calidad compartidas entre silver_stream.py (que las aplica al
limpiar Bronze -> Silver) y el futuro test_silver_quality.py (que las vuelve
a chequear sobre el resultado, Fase 2 - task 8, "obligatorio no opcional").

Un solo lugar de verdad: si un rango cambia, cambia acá y en los dos lugares
que lo usan a la vez — nunca hardcodeado por separado en cada script.
"""

# Extremos físicos razonables para la región Este de EE. UU. (PJME).
# No son los récords históricos exactos — son un margen amplio para
# descartar basura obvia (sensores rotos, bugs de parseo) sin descartar
# datos reales aunque sean inusuales.
WEATHER_RANGES = {
    "temperature_celsius": (-40.0, 55.0),
    "humidity_percentage": (0, 100),
    "wind_speed_m_s": (0.0, 100.0),
}

# El pico histórico real de PJME ronda los ~165,000 MW (2006). 200,000 deja
# margen sin aceptar valores absurdos (negativos, o millones de MW).
ENERGY_MW_RANGE = (0.0, 200_000.0)
