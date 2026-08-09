"""
silver_stream.py — Fase 2 del roadmap.

Lee las tablas Delta de Bronze (weather, energy) *como streams* — no de
Kafka — y escribe la versión limpia en Silver: timestamp tipado (no string),
deduplicado, filtrado a rangos físicamente válidos. Ver docs/architecture-
decisions.md ADR-005 y ADR-006 para el porqué de la separación Bronze/Silver.

Por qué lee de Bronze y no de Kafka de nuevo: desacopla Silver de la
retención de Kafka. Si hay que reprocesar Silver por un bug de limpieza,
Bronze ya tiene el historial completo guardado — no depende de que Kafka
todavía retenga esos mensajes.

Requiere que bronze_stream.py ya haya escrito al menos un micro-batch (si
las tablas de Bronze todavía no existen, esto falla al arrancar).

Uso:
    python silver_stream.py
"""

import os
import sys

from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, to_timestamp, year
from pyspark.sql.utils import StreamingQueryException

from common.logging_config import setup_logger
from common.quality_rules import ENERGY_MW_RANGE, WEATHER_RANGES
from common.spark_session import build_spark_session

logger = setup_logger("silver_stream")

TRIGGER_INTERVAL = "10 seconds"

# Cuánto puede "atrasarse" un evento antes de que Spark deje de esperar
# posibles duplicados suyos y libere ese estado de memoria. Es tiempo de
# EVENTO, no de reloj. Para weather (stream real, infinito mientras el
# productor corra) 10 minutos es generoso: los 5 mensajes de cada ciclo
# llegan separados por milisegundos en tiempo real.
WEATHER_WATERMARK_DELAY = "10 minutes"


def clean_weather(bronze_df: DataFrame) -> DataFrame:
    typed = bronze_df.withColumn("event_time", to_timestamp(col("timestamp")))

    deduped = typed.withWatermark("event_time", WEATHER_WATERMARK_DELAY).dropDuplicatesWithinWatermark(
        ["city", "event_time"]
    )

    # .between() ya excluye nulls (null no cumple ninguna comparación), así
    # que estos filtros hacen doble trabajo: sacan nulls Y sacan rangos
    # imposibles, sin necesitar un isNotNull() separado por columna.
    temp_lo, temp_hi = WEATHER_RANGES["temperature_celsius"]
    hum_lo, hum_hi = WEATHER_RANGES["humidity_percentage"]
    wind_lo, wind_hi = WEATHER_RANGES["wind_speed_m_s"]

    valid = (
        deduped.filter(col("city").isNotNull() & col("event_time").isNotNull())
        .filter(col("temperature_celsius").between(temp_lo, temp_hi))
        .filter(col("humidity_percentage").between(hum_lo, hum_hi))
        .filter(col("wind_speed_m_s").between(wind_lo, wind_hi))
    )

    return valid.select(
        "event_time",
        "city",
        "country",
        "temperature_celsius",
        "humidity_percentage",
        "wind_speed_m_s",
        "condition",
    ).withColumn("silver_processed_at", current_timestamp())


def clean_energy(bronze_df: DataFrame) -> DataFrame:
    """A diferencia de weather, acá NO se usa watermark.

    Bug real encontrado el 2026-08-05: con watermark, cualquier fila que
    llegue "fuera de orden" en más del delay configurado se trata como dato
    tarde y se DESCARTA — no solo se libera memoria vieja, eso es lo que
    realmente hace un watermark en Structured Streaming. PJME_hourly.csv
    (el dataset de Kaggle) NO está ordenado cronológicamente de punta a
    punta (6041 "retrocesos" verificados), así que un watermark de
    cualquier tamaño chico tira filas válidas por error.

    La solución real: energy-stream es un stream ACOTADO (el CSV se termina
    en algún momento), no infinito como weather. Para una fuente acotada de
    ~145k filas, dropDuplicates() sin watermark es seguro — el estado que
    Spark tiene que recordar es como mucho ~145k claves (grid_region,
    event_time), trivial en memoria — a cambio de no poder purgar ese
    estado nunca, que acá no importa porque el stream tiene un final.
    """
    typed = bronze_df.withColumn("event_time", to_timestamp(col("timestamp")))

    deduped = typed.dropDuplicates(["grid_region", "event_time"])

    mw_lo, mw_hi = ENERGY_MW_RANGE

    valid = deduped.filter(col("grid_region").isNotNull() & col("event_time").isNotNull()).filter(
        col("consumption_mw").between(mw_lo, mw_hi)
    )

    # event_year existe solo para particionar el Delta en disco (ver
    # PARTITION_COLUMNS más abajo). PJME_hourly.csv cubre 2002-2018: partir
    # por año da ~16 carpetas, un tamaño de partición razonable (no como
    # particionar por día, que con datos horarios daría miles de carpetas
    # chiquitas — "small file problem").
    return (
        valid.select("event_time", "grid_region", "consumption_mw")
        .withColumn("event_year", year(col("event_time")))
        .withColumn("silver_processed_at", current_timestamp())
    )


CLEANERS = {
    "weather": clean_weather,
    "energy": clean_energy,
}

# A diferencia de Bronze (particionado por ingest_date, tiempo de llegada),
# Silver es la capa de negocio: acá lo que importa es cómo la va a consultar
# alguien después ("dame el clima de Baltimore", "dame el consumo de 2015").
# Particionar por esas columnas permite "partition pruning" — Spark ni
# siquiera abre las carpetas de años/ciudades que la query no pide.
PARTITION_COLUMNS = {
    "weather": ["city"],
    "energy": ["event_year"],
}


def start_silver_query(spark, *, name: str, base_path: str):
    bronze_path = os.path.join(base_path, "bronze", name)
    silver_path = os.path.join(base_path, "silver", name)
    checkpoint_path = os.path.join(base_path, "_checkpoint", f"silver_{name}")

    if not os.path.isdir(bronze_path):
        raise FileNotFoundError(
            f"No existe la tabla Bronze en '{bronze_path}'. "
            "Corré bronze_stream.py primero y dejalo escribir al menos un micro-batch."
        )

    bronze_stream = spark.readStream.format("delta").load(bronze_path)
    clean_df = CLEANERS[name](bronze_stream)

    query = (
        clean_df.writeStream.format("delta")
        .partitionBy(*PARTITION_COLUMNS[name])
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(silver_path)
    )

    logger.info("Query Silver arrancada: %s -> %s (checkpoint=%s)", name, silver_path, checkpoint_path)
    return query


def run() -> None:
    load_dotenv()

    base_path = os.environ.get(
        "LAKEHOUSE_BASE_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "lakehouse"),
    )

    logger.info("silver_stream iniciado. lakehouse_base_path=%s", base_path)

    spark = build_spark_session("silver-stream")

    try:
        weather_query = start_silver_query(spark, name="weather", base_path=base_path)
        energy_query = start_silver_query(spark, name="energy", base_path=base_path)
    except (FileNotFoundError, StreamingQueryException) as exc:
        logger.error("No se pudo arrancar alguna de las queries de Silver: %s", exc)
        spark.stop()
        sys.exit(1)

    logger.info("Las dos queries de Silver están corriendo. Ctrl+C para detener.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Señal de apagado recibida. Deteniendo las queries...")
    except StreamingQueryException as exc:
        logger.error("Una query de streaming falló: %s", exc)
    finally:
        for q in (weather_query, energy_query):
            if q.isActive:
                q.stop()
        spark.stop()
        logger.info("silver_stream detenido.")


if __name__ == "__main__":
    run()
