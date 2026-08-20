"""
Construye (o refresca) transformation/gold.duckdb: un catálogo DuckDB con
vistas de solo lectura sobre las tablas Gold que dbt ya materializó en
transformation/dbt_project/spark-warehouse/.

Por qué un catálogo aparte, en vez de que agent.py apunte directo a los
archivos Parquet/Delta:

1. El agente solo necesita conocer nombres de tabla (fct_..., dim_city),
   igual que si hablara con cualquier base real — no rutas de archivos de
   Spark, que son un detalle interno de cómo dbt escribió Gold.
2. Permite abrir este archivo con read_only=True desde agent.py. Esa es la
   restricción real "a nivel de motor de datos" que pide
   docs/agent-layer-spec.md (Agente 2, guardrail central) — no una
   instrucción de prompt que un mensaje raro podría eludir. Si algo intenta
   escribir sobre una conexión read-only, DuckDB lo rechaza a nivel de
   motor, no porque el prompt se lo pidió.

Este script NO corre dbt por vos, ni copia datos — solo declara vistas que
apuntan a los archivos que dbt ya dejó escritos. Correlo con:
    python build_gold_catalog.py
después de cada `dbt build` (o antes de usar el agente por primera vez).
Las vistas se re-evalúan en cada consulta, así que si volvés a correr
`dbt build` no hace falta reconstruir el catálogo — solo si aparece o
desaparece una tabla completa.
"""

import os
import sys

import duckdb
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from logging_config import setup_logger  # noqa: E402

load_dotenv()
logger = setup_logger("nl_query_agent.build_gold_catalog")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(HERE))

SPARK_WAREHOUSE_PATH = os.environ.get(
    "SPARK_WAREHOUSE_PATH",
    os.path.join(PROJECT_ROOT, "transformation", "dbt_project", "spark-warehouse"),
)
GOLD_DUCKDB_PATH = os.environ.get(
    "GOLD_DUCKDB_PATH",
    os.path.join(PROJECT_ROOT, "transformation", "gold.duckdb"),
)

# Tablas Parquet nativas (ver ADR-010) — dbt las reconstruye enteras en cada
# `dbt build` con CREATE OR REPLACE TABLE, así que la vista solo apunta a la
# carpeta, nunca copiamos datos hacia gold.duckdb.
PARQUET_TABLES = [
    "fct_energy_consumption_hourly",
    "fct_weather_readings",
    "fct_hourly_climate_demand_pattern",
]

# dim_city quedó en Delta (es un seed, nunca pisó el bug de ADR-010).
# DuckDB la lee con su propia extensión "delta" — sin pasar por Spark/JVM
# para nada, justo el punto de ADR-011.
DELTA_TABLES = ["dim_city"]


def build_catalog() -> None:
    if not os.path.isdir(SPARK_WAREHOUSE_PATH):
        logger.error(
            "No encontré spark-warehouse en '%s'. Corré `dbt build` primero "
            "(ver transformation/README.md) antes de construir el catálogo.",
            SPARK_WAREHOUSE_PATH,
        )
        sys.exit(1)

    conn = duckdb.connect(GOLD_DUCKDB_PATH, read_only=False)

    try:
        conn.execute("INSTALL delta;")
        conn.execute("LOAD delta;")

        for table in PARQUET_TABLES:
            table_path = os.path.join(SPARK_WAREHOUSE_PATH, table)
            if not os.path.isdir(table_path):
                logger.warning("Tabla Gold '%s' no existe todavía, la salteo.", table)
                continue
            glob_path = os.path.join(table_path, "*.parquet").replace("\\", "/")
            conn.execute(
                f"CREATE OR REPLACE VIEW {table} AS "
                f"SELECT * FROM read_parquet('{glob_path}')"
            )
            logger.info("Vista '%s' creada sobre %s", table, glob_path)

        for table in DELTA_TABLES:
            table_path = os.path.join(SPARK_WAREHOUSE_PATH, table).replace("\\", "/")
            if not os.path.isdir(table_path):
                logger.warning("Tabla Gold '%s' no existe todavía, la salteo.", table)
                continue
            conn.execute(
                f"CREATE OR REPLACE VIEW {table} AS "
                f"SELECT * FROM delta_scan('{table_path}')"
            )
            logger.info("Vista '%s' creada (Delta) sobre %s", table, table_path)

        tables = conn.execute("SHOW TABLES").fetchall()
        logger.info(
            "Catálogo listo en '%s'. Vistas disponibles: %s",
            GOLD_DUCKDB_PATH,
            [t[0] for t in tables],
        )
    except duckdb.Error:
        logger.exception("Fallo construyendo el catálogo Gold.")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    build_catalog()
