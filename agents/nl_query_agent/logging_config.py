"""
Logging compartido por agents/nl_query_agent/ — copia deliberada de
ingestion/common/logging_config.py y streaming/common/logging_config.py,
no un import cruzado. Mismo motivo que en esos dos: agents/ es otra unidad
que el día de mañana se podría desplegar sola (ej. como servicio propio),
así que no la acoplamos a las otras carpetas por ~40 líneas de código.
"""

import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")


def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        safe_name = name.replace(".", "_")
        file_handler = RotatingFileHandler(
            os.path.join(LOG_DIR, f"{safe_name}.log"),
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError:
        logger.warning("No se pudo crear el log en archivo; continúo solo con consola.")

    return logger
