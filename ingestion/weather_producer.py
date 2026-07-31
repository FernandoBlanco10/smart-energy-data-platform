"""
weather_producer.py — Fase 1 del roadmap.

Consulta OpenWeatherMap cada POLL_INTERVAL_SECONDS para las 5 ciudades de la
región PJME y publica cada lectura en el tópico Kafka `weather-stream`.

Por qué pasa por Kafka en vez de escribir directo a Spark/Delta: ADR-003
(docs/architecture-decisions.md) — desacopla la disponibilidad del pipeline
de la disponibilidad de la API externa.

Uso:
    python weather_producer.py

Requiere (ver .env.example):
    OPENWEATHER_API_KEY
    KAFKA_BOOTSTRAP_SERVERS
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

from common.kafka_utils import get_producer, make_delivery_callback
from common.logging_config import setup_logger

logger = setup_logger("weather_producer")

TOPIC = "weather-stream"
POLL_INTERVAL_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = 10
OWM_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Región Este de EE. UU. (PJME) — project-brief.md, sección 3.A
CITIES = [
    {"city": "Philadelphia", "query": "Philadelphia,PA,US"},
    {"city": "Newark", "query": "Newark,NJ,US"},
    {"city": "Baltimore", "query": "Baltimore,MD,US"},
    {"city": "Wilmington", "query": "Wilmington,DE,US"},
    {"city": "Washington", "query": "Washington,DC,US"},
]

_shutdown_requested = False


def _handle_shutdown(signum, frame):
    global _shutdown_requested
    logger.info("Señal de apagado recibida (%s). Terminando tras el ciclo actual...", signum)
    _shutdown_requested = True


def fetch_weather(city_query: str, api_key: str) -> dict | None:
    """Llama a OpenWeatherMap para una ciudad. Devuelve el JSON crudo o None si falla.

    No relanza la excepción: una ciudad caída no debe tumbar el productor completo
    ni frenar la lectura de las demás ciudades en el mismo ciclo.
    """
    params = {"q": city_query, "appid": api_key, "units": "metric"}
    try:
        response = requests.get(OWM_BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.error("Timeout consultando OpenWeatherMap para %s", city_query)
    except requests.exceptions.HTTPError as exc:
        logger.error("Error HTTP de OpenWeatherMap para %s: %s", city_query, exc)
    except requests.exceptions.RequestException as exc:
        logger.error("Error de red consultando OpenWeatherMap para %s: %s", city_query, exc)
    except ValueError as exc:
        # response.json() con body no-JSON
        logger.error("Respuesta no-JSON de OpenWeatherMap para %s: %s", city_query, exc)
    return None


def build_message(city_name: str, raw: dict) -> dict | None:
    """Mapea la respuesta cruda de OWM al esquema del proyecto
    (project-brief.md, sección 3.A). Devuelve None si faltan campos esperados.
    """
    try:
        return {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "city": city_name,
            "country": "US",
            "temperature_celsius": round(float(raw["main"]["temp"]), 2),
            "humidity_percentage": int(raw["main"]["humidity"]),
            "wind_speed_m_s": round(float(raw["wind"]["speed"]), 2),
            "condition": raw["weather"][0]["main"],
        }
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        logger.error("Respuesta de OWM con forma inesperada para %s: %s | payload=%s", city_name, exc, raw)
        return None


def run() -> None:
    load_dotenv()

    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key:
        logger.error(
            "OPENWEATHER_API_KEY no está definida. Completá tu .env "
            "(conseguí una key gratis en https://openweathermap.org/api) y reintentá."
        )
        sys.exit(1)

    try:
        producer = get_producer(client_id="weather-producer")
    except RuntimeError as exc:
        logger.error("No se pudo inicializar el productor de Kafka: %s", exc)
        sys.exit(1)

    on_delivery = make_delivery_callback(logger)

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    logger.info(
        "weather_producer iniciado. Ciudades=%d, intervalo=%ds, tópico=%s",
        len(CITIES),
        POLL_INTERVAL_SECONDS,
        TOPIC,
    )

    while not _shutdown_requested:
        cycle_start = time.monotonic()
        published = 0

        for entry in CITIES:
            raw = fetch_weather(entry["query"], api_key)
            if raw is None:
                continue

            message = build_message(entry["city"], raw)
            if message is None:
                continue

            try:
                producer.produce(
                    topic=TOPIC,
                    key=entry["city"].encode("utf-8"),
                    value=json.dumps(message).encode("utf-8"),
                    on_delivery=on_delivery,
                )
                producer.poll(0)  # dispara callbacks pendientes sin bloquear
                published += 1
            except BufferError:
                logger.warning("Cola local del productor llena; hago flush antes de seguir.")
                producer.flush(timeout=10)
            except Exception as exc:  # noqa: BLE001 — un fallo de producción no debe tumbar el loop
                logger.error("Error inesperado publicando %s: %s", entry["city"], exc)

        producer.flush(timeout=10)
        logger.info("Ciclo completo: %d/%d ciudades publicadas.", published, len(CITIES))

        if _shutdown_requested:
            break

        elapsed = time.monotonic() - cycle_start
        sleep_for = max(0.0, POLL_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_for)

    logger.info("Apagando productor limpiamente...")
    producer.flush(timeout=10)
    logger.info("weather_producer detenido.")


if __name__ == "__main__":
    run()
