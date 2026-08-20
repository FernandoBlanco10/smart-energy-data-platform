"""
Agente 2 (docs/agent-layer-spec.md) — consulta en lenguaje natural sobre la
capa Gold. Recibe una pregunta en español/inglés, la Claude API genera SQL
de solo lectura vía tool-use, ese SQL se ejecuta contra gold.duckdb, y el
modelo devuelve una explicación en texto del resultado — no solo el dato
crudo.

Dos capas de guardrail, no una sola (ver docs/agent-layer-spec.md, Agente 2
y "Guardrails transversales"):

1. validate_select_only(): rechaza cualquier SQL que no empiece con
   SELECT/WITH o que contenga una palabra clave de escritura, ANTES de
   tocar la base. Esto es un chequeo rápido, pero es un guardrail de
   *código*, no de prompt — no depende de que el modelo "decida portarse
   bien".
2. La conexión a gold.duckdb se abre con read_only=True (ver
   build_gold_catalog.py). Esta es la restricción real "a nivel de motor
   de datos" que pide la spec: aunque algo se colara más allá del punto 1,
   DuckDB va a rechazar cualquier escritura por su cuenta.

Además: cada pregunta + SQL ejecutado queda logueado (logs/), y el loop de
tool-use tiene un límite duro de iteraciones (MAX_TOOL_ITERATIONS) — ningún
agente de este proyecto reintenta indefinido (guardrail transversal #3).
"""

import json
import os
import re
import sys

import duckdb
from anthropic import Anthropic
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from logging_config import setup_logger  # noqa: E402

load_dotenv()
logger = setup_logger("nl_query_agent.agent")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(HERE))

GOLD_DUCKDB_PATH = os.environ.get(
    "GOLD_DUCKDB_PATH",
    os.path.join(PROJECT_ROOT, "transformation", "gold.duckdb"),
)
MODEL = os.environ.get("NL_QUERY_AGENT_MODEL", "claude-sonnet-5")
MAX_TOOL_ITERATIONS = 5
MAX_ROWS_RETURNED = 200

GOLD_SCHEMA = """
fct_energy_consumption_hourly — consumo eléctrico por hora, red PJME (histórico 2002-2018).
  event_time (timestamp, un valor único por fila), grid_region (siempre 'PJME'),
  consumption_mw (double), prev_hour_consumption_mw, next_hour_consumption_mw,
  rolling_24h_avg_consumption_mw, hour_over_hour_change_mw.

fct_weather_readings — lecturas de clima en vivo por ciudad, enriquecidas con dim_city.
  event_time (timestamp), city (FK a dim_city), temperature_celsius,
  humidity_percentage, wind_speed_m_s, condition (categoría OpenWeatherMap:
  Clear, Clouds, Rain, etc.), state, region (de dim_city).

fct_hourly_climate_demand_pattern — patrón agregado por HORA DEL DÍA (0-23),
  no por fecha exacta: energy es histórico (2002-2018) y weather es en vivo
  (hoy), no comparten un timestamp real, así que se comparan por hora del
  día en vez de fecha. hour_of_day (0-23, un valor único por fila),
  avg_consumption_mw, n_energy_readings, avg_temperature_celsius,
  n_weather_readings.

dim_city — las 5 ciudades de la región PJME (Philadelphia, Newark,
  Baltimore, Wilmington, Washington; todas PA/NJ/MD/DE/DC, US).
  city (un valor único por fila), state, country, region.
""".strip()

SYSTEM_PROMPT = f"""Eres un analista de datos que responde preguntas sobre la
capa Gold de un pipeline de energía y clima (PJME + OpenWeatherMap), en
español o inglés según el idioma de la pregunta.

Esquema disponible (DuckDB, todo en el catálogo "default"):

{GOLD_SCHEMA}

Reglas estrictas:
- Solo podés generar sentencias SELECT (o WITH ... SELECT). Nunca vas a
  poder ejecutar otra cosa igual: la conexión está abierta en modo
  read-only, cualquier otra sentencia el motor la va a rechazar sola.
- Nunca inventes columnas o tablas que no están en el esquema de arriba.
- Si la pregunta no se puede responder con este esquema (ej. pide un dato
  de Bronze/Silver, o algo que no está modelado en Gold), decilo
  explícitamente en vez de inventar una respuesta.
- Después de ver el resultado de tu consulta, respondé en texto plano:
  explicá qué significa el resultado, no solo repitas el número crudo.
"""

TOOLS = [
    {
        "name": "run_sql_query",
        "description": (
            "Ejecuta una sentencia SQL de solo lectura (SELECT) contra las "
            "tablas Gold, en sintaxis DuckDB. La conexión es read-only a "
            "nivel de motor — cualquier intento de escritura es rechazado "
            "por DuckDB, no por esta herramienta."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "Sentencia SELECT (o WITH ... SELECT) en sintaxis DuckDB.",
                }
            },
            "required": ["sql"],
        },
    }
]

_FORBIDDEN_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|COPY|ATTACH|"
    r"DETACH|PRAGMA|INSTALL|LOAD|EXPORT|IMPORT|CALL|VACUUM|CHECKPOINT|SET)\b",
    re.IGNORECASE,
)


def validate_select_only(sql: str) -> None:
    """Primera capa de guardrail — ver docstring del módulo."""
    stripped = sql.strip().rstrip(";")
    if not stripped:
        raise ValueError("SQL vacío.")
    if not re.match(r"^(WITH|SELECT)\b", stripped, re.IGNORECASE):
        raise ValueError("Solo se permiten sentencias SELECT (o WITH ... SELECT).")
    if _FORBIDDEN_KEYWORDS.search(stripped):
        raise ValueError("La consulta contiene una palabra clave no permitida.")


def _run_tool(conn: duckdb.DuckDBPyConnection, sql: str) -> str:
    try:
        validate_select_only(sql)
        cursor = conn.execute(sql)
        columns = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        truncated = len(rows) > MAX_ROWS_RETURNED
        payload = {
            "columns": columns,
            "rows": rows[:MAX_ROWS_RETURNED],
            "truncated": truncated,
        }
        return json.dumps(payload, default=str)
    except (duckdb.Error, ValueError) as exc:
        logger.warning("SQL rechazado o falló: %s | sql=%s", exc, sql)
        return f"ERROR: {exc}"


def ask(question: str) -> str:
    if not os.path.exists(GOLD_DUCKDB_PATH):
        return (
            f"No encontré el catálogo Gold en '{GOLD_DUCKDB_PATH}'. "
            "Corré primero: python build_gold_catalog.py"
        )

    client = Anthropic()  # lee ANTHROPIC_API_KEY del entorno
    conn = duckdb.connect(GOLD_DUCKDB_PATH, read_only=True)

    messages = [{"role": "user", "content": question}]
    final_text = None

    try:
        for _ in range(MAX_TOOL_ITERATIONS):
            response = client.messages.create(
                model=MODEL,
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            )

            tool_uses = [block for block in response.content if block.type == "tool_use"]

            if not tool_uses:
                final_text = "".join(
                    block.text for block in response.content if block.type == "text"
                )
                break

            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in tool_uses:
                sql = block.input.get("sql", "")
                logger.info("pregunta=%r sql=%r", question, sql)
                result_content = _run_tool(conn, sql)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_content,
                    }
                )

            messages.append({"role": "user", "content": tool_results})
        else:
            final_text = (
                f"No pude resolver la pregunta en {MAX_TOOL_ITERATIONS} intentos — "
                "corto acá en vez de seguir reintentando (ver agent-layer-spec.md, "
                "guardrail transversal #3)."
            )
    finally:
        conn.close()

    return final_text or "El modelo no devolvió una respuesta en texto."


def main() -> None:
    if len(sys.argv) > 1:
        print(ask(" ".join(sys.argv[1:])))
        return

    print("Agente NL->SQL sobre la capa Gold. Escribi 'salir' para terminar.\n")
    while True:
        try:
            question = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not question or question.lower() in {"salir", "exit", "quit"}:
            break
        print()
        print(ask(question))
        print()


if __name__ == "__main__":
    main()
