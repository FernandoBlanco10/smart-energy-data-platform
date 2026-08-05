"""
Esquemas del payload JSON que publican los productores de ingestion/.

Viven acá (no en ingestion/) porque quien los necesita es Spark, para poder
usar from_json() y convertir los bytes crudos del value de Kafka en columnas
tipadas. Si el productor cambia el esquema, este archivo es lo primero que
hay que actualizar — y solo acá, no en cada job que lee el tópico.
"""

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Debe coincidir con build_message() en ingestion/weather_producer.py
WEATHER_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("country", StringType(), nullable=True),
        StructField("temperature_celsius", DoubleType(), nullable=True),
        StructField("humidity_percentage", IntegerType(), nullable=True),
        StructField("wind_speed_m_s", DoubleType(), nullable=True),
        StructField("condition", StringType(), nullable=True),
    ]
)

# Debe coincidir con build_message() en ingestion/energy_producer.py
ENERGY_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), nullable=False),
        StructField("grid_region", StringType(), nullable=False),
        StructField("consumption_mw", DoubleType(), nullable=True),
    ]
)
