# Datos de entrada

`PJME_hourly.csv` **no está versionado** (ver `.gitignore` — todo `*.csv` queda afuera a propósito, es un dataset de terceros, no código).

Descargalo de Kaggle: [robikscube/hourly-energy-consumption](https://www.kaggle.com/datasets/robikscube/hourly-energy-consumption) — el archivo específico es `PJME_hourly.csv`. Necesitás cuenta de Kaggle (gratis) para bajarlo.

Poné el archivo acá mismo: `ingestion/data/PJME_hourly.csv`. `energy_producer.py` lo espera en esa ruta por defecto (configurable con la variable de entorno `ENERGY_CSV_PATH`).

Columnas esperadas: `Datetime`, `PJME_MW` (así viene el dataset original de Kaggle, sin modificar).
