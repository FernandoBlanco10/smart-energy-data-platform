"""
Helpers compartidos para crear productores de Kafka y confirmar entregas.

Usa confluent-kafka (basado en librdkafka) — es el cliente que vas a encontrar
en la gran mayoría de stacks de Kafka en producción.
"""

import logging
import os

from confluent_kafka import KafkaError, Producer


def build_producer_config(client_id: str) -> dict:
    """Arma la config del productor a partir de variables de entorno.

    KAFKA_BOOTSTRAP_SERVERS viene de .env (nunca hardcodeado). acks='all'
    y enable.idempotence=True priorizan no perder ni duplicar mensajes por
    sobre latencia mínima — para métricas de consumo eléctrico y clima,
    perder un mensaje silenciosamente es peor que un pequeño overhead.
    """
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        raise RuntimeError(
            "KAFKA_BOOTSTRAP_SERVERS no está definido. Revisá tu archivo .env "
            "(ver .env.example)."
        )

    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "retry.backoff.ms": 500,
    }


def get_producer(client_id: str) -> Producer:
    return Producer(build_producer_config(client_id))


def make_delivery_callback(logger: logging.Logger):
    """Devuelve un callback de entrega que solo loguea — nunca lanza excepciones,
    porque corre dentro del poll loop del productor, no del hilo principal."""

    def _on_delivery(err: KafkaError, msg) -> None:
        if err is not None:
            logger.error("Falló la entrega a Kafka: %s (tópico=%s)", err, msg.topic())
        else:
            logger.debug(
                "Entregado a %s [partición %s] offset %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    return _on_delivery
