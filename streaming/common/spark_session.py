"""
Construcción compartida de la SparkSession para todos los jobs de streaming/.

Centraliza la config de Delta Lake y del conector de Kafka acá para que
bronze_stream.py, silver_stream.py, etc. no dupliquen esta configuración
(mismo motivo que ingestion/common/: un solo lugar para arreglar versiones).
"""

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_spark_session(app_name: str) -> SparkSession:
    """Arma una SparkSession local con Delta Lake y el conector de Kafka listos.

    Las versiones del conector de Kafka y del sufijo de Scala son
    configurables por variable de entorno por si en algún momento conviene
    subir de nuevo a Spark 4.x/Scala 2.13 (ver build-log.md — se bajó a esta
    combinación por falta de soporte de winutils.exe en Windows para
    Hadoop 3.4.0, no por un problema del código en sí).
    """
    kafka_connector_version = os.environ.get("SPARK_KAFKA_CONNECTOR_VERSION", "3.5.8")
    scala_suffix = os.environ.get("SPARK_SCALA_SUFFIX", "2.12")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Default de Spark es 200 particiones de shuffle — pensado para un
        # clúster, no para un laptop. Con pocas particiones locales, 200
        # shuffles vacíos solo agregan overhead.
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
    )

    # OJO acá: configure_spark_with_delta_pip() arma su propio
    # spark.jars.packages para los jars de Delta — si vos también seteás
    # spark.jars.packages en el builder (como hacía antes), lo PISA en vez
    # de sumarlo, y el conector de Kafka desaparece en silencio. El paquete
    # de Kafka se pasa acá, vía extra_packages, que sí lo concatena.
    kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_suffix}:{kafka_connector_version}"
    spark = configure_spark_with_delta_pip(builder, extra_packages=[kafka_package]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # los INFO de Spark son muchísimo ruido
    return spark
