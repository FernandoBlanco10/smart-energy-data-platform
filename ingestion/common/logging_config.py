"""
Configuración de logging compartida por todos los productores de ingestion/.

Regla del proyecto (.context/project-brief.md, sección 4): nunca usar print(),
todo va a través de logging, con manejo de excepciones explícito en quien lo usa.
"""

import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")


def setup_logger(name: str) -> logging.Logger:
    """Crea (o recupera) un logger con salida a consola y a archivo rotativo.

    Args:
        name: nombre del logger, típicamente __name__ del módulo que lo llama.
              También se usa como nombre del archivo de log.

    Returns:
        Logger configurado, listo para usar. Idempotente: si se llama varias
        veces con el mismo name no duplica handlers.
    """
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
            maxBytes=5 * 1024 * 1024,  # 5MB por archivo
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError:
        # Si no se puede escribir a disco (permisos, disco lleno), seguimos
        # solo con consola en vez de tumbar el productor por un problema de logging.
        logger.warning("No se pudo crear el log en archivo; continúo solo con consola.")

    return logger
