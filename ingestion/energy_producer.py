"""
energy_producer.py — Fase 2 del roadmap.

Lee PJME_hourly.csv secuencialmente y publica 1 fila por segundo en el
tópico Kafka `energy-stream` — simula el consumo horario real de la red
PJME como si fuera un stream en tiempo real (1 hora real de consumo = 1
segundo simulado).

Por qué pasa por Kafka en vez de que Spark lea el CSV directo: para que el
pipeline de Fase 2 (Spark Structured Streaming) trate esta fuente batch
exactamente igual que el clima en tiempo real — mismo contrato, un solo
tipo de consumidor. ADR-003 (docs/architecture-decisions.md).

Uso:
    python energy_producer.py

Requiere (ver .env.example):
    KAFKA_BOOTSTRAP_SERVERS
    ENERGY_CSV_PATH (opcional, default: data/PJME_hourly.csv)
"""

import csv
import json
import os
import signal
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

from common.kafka_utils import get_producer, make_delivery_callback
from common.logging_config import setup_logger

logger = setup_logger("energy_producer")

TOPIC = "energy-stream"
GRID_REGION = "PJME"
# 1.0 = "1 hora real de consumo = 1 segundo simulado" (ver docstring). A ese
# ritmo, publicar el CSV completo (145,367 filas) tarda ~40 horas. Para
# ejercicios de observación (ej. ver cómo se forman las particiones por año
# en Silver) sirve acelerarlo con ENERGY_SECONDS_PER_ROW=0.02 (~2000 filas
# en menos de un minuto) sin tocar el comportamiento por defecto del resto
# del proyecto.
SECONDS_PER_ROW = float(os.environ.get("ENERGY_SECONDS_PER_ROW", "1.0"))
DEFAULT_CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "PJME_hourly.csv")

# El CSV de Kaggle (robikscube/hourly-energy-consumption) trae el timestamp
# en un formato tipo "2002-01-01 01:00:00" — ya coincide con el esquema del
# proyecto, pero lo parseamos igual para validar que cada fila es una fecha
# real antes de publicarla (ver build_message).
CSV_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

_shutdown_requested = False


def _handle_shutdown(signum, frame):
    global _shutdown_requested
    logger.info("Señal de apagado recibida (%s). Terminando tras la fila actual...", signum)
    _shutdown_requested = True


def build_message(row: dict) -> dict | None:
    """Mapea una fila cruda del CSV al esquema del proyecto
    (project-brief.md, sección 3.B). Devuelve None si la fila es inválida
    en vez de lanzar una excepción — una fila corrupta no debe tumbar todo
    el productor, solo se salta y se loguea.
    """
    try:
        raw_timestamp = row["Datetime"].strip()
        raw_mw = row["PJME_MW"].strip()

        parsed_ts = datetime.strptime(raw_timestamp, CSV_DATETIME_FORMAT)
        consumption = float(raw_mw)

        return {
            "timestamp": parsed_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "grid_region": GRID_REGION,
            "consumption_mw": round(consumption, 2),
        }
    except (KeyError, ValueError) as exc:
        logger.error("Fila inválida en el CSV, se descarta: %s | fila=%s", exc, row)
        return None


def run() -> None:
    load_dotenv()

    csv_path = os.environ.get("ENERGY_CSV_PATH", DEFAULT_CSV_PATH)
    if not os.path.isfile(csv_path):
        logger.error(
            "No encuentro el CSV en '%s'. Descargalo de Kaggle "
            "(robikscube/hourly-energy-consumption) y poné PJME_hourly.csv ahí "
            "— ver ingestion/data/README.md.",
            csv_path,
        )
        sys.exit(1)

    try:
        producer = get_producer(client_id="energy-producer")
    except RuntimeError as exc:
        logger.error("No se pudo inicializar el productor de Kafka: %s", exc)
        sys.exit(1)

    on_delivery = make_delivery_callback(logger)

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    logger.info(
        "energy_producer iniciado. csv=%s, ritmo=%ss/fila, tópico=%s",
        csv_path,
        SECONDS_PER_ROW,
        TOPIC,
    )

    published = 0
    skipped = 0

    try:
        with open(csv_path, newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)

            for row in reader:
                if _shutdown_requested:
                    break

                message = build_message(row)
                if message is None:
                    skipped += 1
                    continue

                try:
                    producer.produce(
                        topic=TOPIC,
                        key=GRID_REGION.encode("utf-8"),
                        value=json.dumps(message).encode("utf-8"),
                        on_delivery=on_delivery,
                    )
                    producer.poll(0)
                    published += 1
                except BufferError:
                    logger.warning("Cola local del productor llena; hago flush antes de seguir.")
                    producer.flush(timeout=10)
                except Exception as exc:  # noqa: BLE001 — no tumbar el loop por un fallo puntual
                    logger.error("Error inesperado publicando fila: %s", exc)
                    skipped += 1

                if published % 500 == 0 and published > 0:
                    logger.info("Progreso: %d filas publicadas, %d descartadas.", published, skipped)

                time.sleep(SECONDS_PER_ROW)
    except OSError as exc:
        logger.error("Error leyendo el CSV: %s", exc)
        sys.exit(1)
    finally:
        producer.flush(timeout=10)

    logger.info(
        "energy_producer terminado. Total publicadas=%d, descartadas=%d.",
        published,
        skipped,
    )


if __name__ == "__main__":
    run()
