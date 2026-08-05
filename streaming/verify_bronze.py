"""
verify_bronze.py — utilidad de verificación, no un job de producción.

Lee las tablas Delta de Bronze (weather y energy) y muestra conteo de filas,
esquema y una muestra — para confirmar en 10 segundos que bronze_stream.py
escribió algo real, sin tener que andar mirando archivos .parquet a mano.

Uso (con bronze_stream.py corrido al menos un par de minutos antes):
    python verify_bronze.py
"""

import os
import sys

from dotenv import load_dotenv

from common.spark_session import build_spark_session


def show_table(spark, name: str, path: str) -> None:
    print(f"\n{'=' * 60}\n{name}  ({path})\n{'=' * 60}")

    if not os.path.isdir(path):
        print("  -> la carpeta no existe todavía. ¿Corriste bronze_stream.py?")
        return

    try:
        df = spark.read.format("delta").load(path)
    except Exception as exc:  # noqa: BLE001 — es un script de verificación manual
        print(f"  -> no se pudo leer como tabla Delta: {exc}")
        return

    count = df.count()
    print(f"  Filas: {count}")

    if count == 0:
        print("  -> la tabla existe pero está vacía. ¿El productor está corriendo?")
        return

    print("\n  Esquema:")
    df.printSchema()

    print("  Últimas 5 filas por ingested_at:")
    df.orderBy(df.ingested_at.desc()).show(5, truncate=False)


def run() -> None:
    load_dotenv()

    base_path = os.environ.get(
        "LAKEHOUSE_BASE_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "lakehouse"),
    )

    spark = build_spark_session("verify-bronze")

    try:
        show_table(spark, "BRONZE — weather", os.path.join(base_path, "bronze", "weather"))
        show_table(spark, "BRONZE — energy", os.path.join(base_path, "bronze", "energy"))
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
