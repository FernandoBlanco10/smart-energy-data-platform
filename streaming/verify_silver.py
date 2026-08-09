"""
verify_silver.py — utilidad de verificación, no un job de producción.

Igual que verify_bronze.py pero para Silver, más un chequeo extra: compara
el conteo de filas contra Bronze para que se vea, en números, cuántas se
descartaron en la limpieza (duplicados + fuera de rango).

Uso (con silver_stream.py corrido al menos un par de minutos antes):
    python verify_silver.py
"""

import os

from dotenv import load_dotenv

from common.spark_session import build_spark_session


def show_table(spark, name: str, path: str) -> int:
    print(f"\n{'=' * 60}\n{name}  ({path})\n{'=' * 60}")

    if not os.path.isdir(path):
        print("  -> la carpeta no existe todavía.")
        return 0

    try:
        df = spark.read.format("delta").load(path)
    except Exception as exc:  # noqa: BLE001 — script de verificación manual
        print(f"  -> no se pudo leer como tabla Delta: {exc}")
        return 0

    count = df.count()
    print(f"  Filas: {count}")

    if count == 0:
        return 0

    print("\n  Esquema:")
    df.printSchema()

    order_col = "silver_processed_at" if "silver_processed_at" in df.columns else "ingested_at"
    print(f"  Últimas 5 filas por {order_col}:")
    df.orderBy(df[order_col].desc()).show(5, truncate=False)

    return count


def run() -> None:
    load_dotenv()

    base_path = os.environ.get(
        "LAKEHOUSE_BASE_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "lakehouse"),
    )

    spark = build_spark_session("verify-silver")

    try:
        for name in ("weather", "energy"):
            bronze_count = show_table(spark, f"BRONZE — {name}", os.path.join(base_path, "bronze", name))
            silver_count = show_table(spark, f"SILVER — {name}", os.path.join(base_path, "silver", name))
            if bronze_count > 0:
                dropped = bronze_count - silver_count
                pct = (dropped / bronze_count) * 100
                print(f"\n  {name}: {dropped} filas descartadas en la limpieza ({pct:.1f}% de Bronze)\n")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
