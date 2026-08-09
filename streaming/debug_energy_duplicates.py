"""
debug_energy_duplicates.py — script de un solo uso, no es parte del pipeline.

Objetivo: entender POR QUÉ energy pasó de 12.9% a 26.3% descartado después
de sacar el watermark. La hipótesis es que no es un bug de dedup — es que
durante esta sesión reiniciamos energy_producer.py varias veces (por el
docker compose down -v, por el error de UnknownTopicOrPartitionException),
y como el productor no guarda ningún estado, cada reinicio vuelve a leer el
CSV desde la fila 1. Eso publica las mismas filas de negocio dos (o más)
veces a Kafka, con offsets distintos pero el mismo event_time — y
dropDuplicates() las está descartando correctamente, porque SÍ son
duplicados reales.

Este script lee Bronze (no Silver) en modo batch, agrupa por event_time,
y para cada event_time repetido muestra los kafka_offset involucrados.
Si los offsets de un mismo event_time están muy separados entre sí (ej. uno
en offset ~50 y otro en offset ~800), confirma que vinieron de corridas
distintas del productor — offsets consecutivos en cambio sugerirían otra
causa.

Uso:
    cd streaming
    python debug_energy_duplicates.py
"""

import os

from dotenv import load_dotenv

from common.spark_session import build_spark_session


def run() -> None:
    load_dotenv()

    base_path = os.environ.get(
        "LAKEHOUSE_BASE_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "lakehouse"),
    )
    bronze_path = os.path.join(base_path, "bronze", "energy")

    spark = build_spark_session("debug-energy-duplicates")

    try:
        df = spark.read.format("delta").load(bronze_path)
        total = df.count()
        print(f"\nTotal filas en Bronze/energy: {total}")

        dupes = (
            df.groupBy("timestamp")
            .count()
            .filter("count > 1")
            .orderBy("count", ascending=False)
        )
        dupe_groups = dupes.count()
        dupe_rows = dupes.selectExpr("sum(count) as total").collect()[0]["total"] or 0
        print(f"Timestamps con más de una fila: {dupe_groups} grupos, {dupe_rows} filas involucradas")

        print("\nTop 10 timestamps más repetidos, con sus kafka_offset:")
        top_timestamps = [row["timestamp"] for row in dupes.limit(10).collect()]
        for ts in top_timestamps:
            offsets = (
                df.filter(df.timestamp == ts)
                .select("kafka_offset")
                .orderBy("kafka_offset")
                .collect()
            )
            offset_list = [r["kafka_offset"] for r in offsets]
            print(f"  {ts}: offsets={offset_list}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
