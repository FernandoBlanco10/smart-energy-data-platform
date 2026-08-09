"""
test_silver_quality.py — Fase 2, task 8 del roadmap: "obligatorio, no opcional".

Distinto de silver_stream.py a propósito: silver_stream.py APLICA la limpieza
(filtra nulls, filtra rangos, deduplica) mientras escribe. Este script no
confía en que esa limpieza haya funcionado — vuelve a chequear las mismas
invariantes de forma independiente, leyendo Silver ya escrito. Si algún día
alguien rompe silver_stream.py (o cambia una regla sin actualizar la otra
parte), este test lo detecta igual, porque no comparte la lógica de
limpieza, solo comparte las reglas de negocio (quality_rules.py).

No es parte del pipeline de streaming — corre una vez, en modo batch, sobre
lo que haya en Silver en ese momento. Pensado para correr manualmente o
como step de CI/pre-commit en un proyecto real.

Uso:
    cd streaming
    python test_silver_quality.py
"""

import os
import sys

from dotenv import load_dotenv
from pyspark.sql import DataFrame

from common.quality_rules import ENERGY_MW_RANGE, WEATHER_RANGES
from common.spark_session import build_spark_session

results = []


def record(table: str, check: str, passed: bool, detail: str = "") -> None:
    results.append({"table": table, "check": check, "passed": passed, "detail": detail})


def check_no_nulls(df: DataFrame, columns: list[str], table: str) -> None:
    for col_name in columns:
        null_count = df.filter(df[col_name].isNull()).count()
        record(
            table,
            f"sin nulls en '{col_name}'",
            null_count == 0,
            "" if null_count == 0 else f"{null_count} filas con {col_name} nulo",
        )


def check_range(df: DataFrame, column: str, lo: float, hi: float, table: str) -> None:
    out_of_range = df.filter(~df[column].between(lo, hi)).count()
    record(
        table,
        f"'{column}' dentro de [{lo}, {hi}]",
        out_of_range == 0,
        "" if out_of_range == 0 else f"{out_of_range} filas fuera de rango",
    )


def check_uniqueness(df: DataFrame, key_columns: list[str], table: str) -> None:
    total = df.count()
    distinct = df.select(*key_columns).distinct().count()
    duplicated = total - distinct
    record(
        table,
        f"unicidad de {tuple(key_columns)}",
        duplicated == 0,
        "" if duplicated == 0 else f"{duplicated} filas duplicadas sobre la clave de negocio",
    )


def run() -> None:
    load_dotenv()

    base_path = os.environ.get(
        "LAKEHOUSE_BASE_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "lakehouse"),
    )

    spark = build_spark_session("test-silver-quality")

    try:
        weather_path = os.path.join(base_path, "silver", "weather")
        energy_path = os.path.join(base_path, "silver", "energy")

        if not os.path.isdir(weather_path) or not os.path.isdir(energy_path):
            print("Falta silver/weather o silver/energy. Corré silver_stream.py primero.")
            sys.exit(1)

        weather_df = spark.read.format("delta").load(weather_path)
        energy_df = spark.read.format("delta").load(energy_path)

        # --- weather ---
        check_no_nulls(weather_df, ["event_time", "city", "temperature_celsius", "humidity_percentage", "wind_speed_m_s"], "weather")
        temp_lo, temp_hi = WEATHER_RANGES["temperature_celsius"]
        hum_lo, hum_hi = WEATHER_RANGES["humidity_percentage"]
        wind_lo, wind_hi = WEATHER_RANGES["wind_speed_m_s"]
        check_range(weather_df, "temperature_celsius", temp_lo, temp_hi, "weather")
        check_range(weather_df, "humidity_percentage", hum_lo, hum_hi, "weather")
        check_range(weather_df, "wind_speed_m_s", wind_lo, wind_hi, "weather")
        check_uniqueness(weather_df, ["city", "event_time"], "weather")

        # --- energy ---
        check_no_nulls(energy_df, ["event_time", "grid_region", "consumption_mw"], "energy")
        mw_lo, mw_hi = ENERGY_MW_RANGE
        check_range(energy_df, "consumption_mw", mw_lo, mw_hi, "energy")
        check_uniqueness(energy_df, ["grid_region", "event_time"], "energy")

    finally:
        spark.stop()

    print(f"\n{'=' * 70}")
    print(f"{'Tabla':<10}{'Check':<45}{'Resultado'}")
    print("=" * 70)
    failed = 0
    for r in results:
        status = "OK" if r["passed"] else "FALLÓ"
        if not r["passed"]:
            failed += 1
        print(f"{r['table']:<10}{r['check']:<45}{status}")
        if r["detail"]:
            print(f"{'':<10}  -> {r['detail']}")
    print("=" * 70)
    print(f"{len(results)} checks corridos, {failed} fallaron.\n")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    run()
