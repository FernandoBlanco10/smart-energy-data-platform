"""
bronze_stream.py — Fase 2 del roadmap.

Job de Spark Structured Streaming que consume `weather-stream` y
`energy-stream` de Kafka y los escribe como tablas Delta en la capa Bronze.

"Crudo" en Bronze significa: sin deduplicar, sin filtrar filas, sin validar
rangos — esas son responsabilidades de Silver. Pero sí convertimos el JSON a
columnas tipadas (from_json), porque una capa Bronze donde todo es un blob de
bytes sin parsear no se puede ni inspeccionar. Ver docs/architecture-decisions.md
ADR-005 para la justificación completa de Bronze/Silver/Gold sobre Delta Lake.

Corre dos streaming queries en paralelo dentro de la misma SparkSession —una
por tópico— y espera a que cualquiera de las dos termine o falle
(spark.streams.awaitAnyTermination), que es lo que mantiene el proceso vivo.

Uso:
    python bronze_stream.py

Requiere (ver .env.example):
    KAFKA_BOOTSTRAP_SERVERS
    LAKEHOUSE_BASE_PATH (opcional, default: ./lakehouse)
"""

import os
import sys

from dotenv import load_dotenv
from pyspark.sql.functions import col, current_timestamp, from_json, to_date
from pyspark.sql.utils import StreamingQueryException

from common.logging_config import setup_logger
from common.schemas import ENERGY_SCHEMA, WEATHER_SCHEMA
from common.spark_session import build_spark_session

logger = setup_logger("bronze_stream")

TRIGGER_INTERVAL = "10 seconds"


def start_bronze_query(spark, *, topic: str, schema, bootstrap_servers: str, base_path: str):
    """Arma y arranca una streaming query Kafka -> Delta Bronze para un tópico.

    No hace .awaitTermination() acá adentro a propósito: devuelve la query
    para que el caller pueda arrancar varias en paralelo y esperarlas juntas.
    """
    bronze_path = os.path.join(base_path, "bronze", topic.replace("-stream", ""))
    checkpoint_path = os.path.join(base_path, "_checkpoint", f"bronze_{topic.replace('-stream', '')}")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")  # en dev, si Kafka podó un segmento viejo, no tumbar el job
        .load()
    )

    parsed = (
        raw.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("ingested_at", current_timestamp())
        # Partición física por fecha de INGESTA, no de evento. Bronze es la
        # zona de aterrizaje: acabamos de aprender con el bug de Silver que
        # el tiempo de evento puede venir desordenado (backfills, CSVs no
        # ordenados, reintentos). ingest_date es monótona por definición
        # (siempre "ahora"), así que particionar por ella nunca se rompe,
        # sin importar qué tan desordenados vengan los datos de origen.
        .withColumn("ingest_date", to_date(col("ingested_at")))
    )

    query = (
        parsed.writeStream.format("delta")
        .partitionBy("ingest_date")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(bronze_path)
    )

    logger.info("Query Bronze arrancada: tópico=%s -> %s (checkpoint=%s)", topic, bronze_path, checkpoint_path)
    return query


def run() -> None:
    load_dotenv()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        logger.error("KAFKA_BOOTSTRAP_SERVERS no está definido. Revisá tu .env.")
        sys.exit(1)

    base_path = os.environ.get(
        "LAKEHOUSE_BASE_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "lakehouse"),
    )

    logger.info("bronze_stream iniciado. lakehouse_base_path=%s", base_path)

    spark = build_spark_session("bronze-stream")

    try:
        weather_query = start_bronze_query(
            spark, topic="weather-stream", schema=WEATHER_SCHEMA, bootstrap_servers=bootstrap_servers, base_path=base_path
        )
        energy_query = start_bronze_query(
            spark, topic="energy-stream", schema=ENERGY_SCHEMA, bootstrap_servers=bootstrap_servers, base_path=base_path
        )
    except StreamingQueryException as exc:
        logger.error("No se pudo arrancar alguna de las queries de Bronze: %s", exc)
        spark.stop()
        sys.exit(1)

    logger.info("Las dos queries de Bronze están corriendo. Ctrl+C para detener.")

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
        logger.info("bronze_stream detenido.")


if __name__ == "__main__":
    run()
