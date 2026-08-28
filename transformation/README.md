# Transformación — dbt sobre Silver → Gold

Ver `docs/architecture-decisions.md` ADR-006 (por qué dbt) y ADR-009 (por qué
`dbt-spark` en modo `session`) para el razonamiento completo.

## Piezas de este directorio

- `dbt_project/` — el proyecto dbt en sí (modelos, sources, tests).
- `spark-defaults.conf` — configura Delta Lake para **cualquier** SparkSession
  que arranque con `SPARK_CONF_DIR` apuntando acá, incluida la que crea
  `dbt-spark` internamente en modo `session` (que no expone un lugar directo
  en `profiles.yml` para pasarle `spark.jars.packages`). Es el mismo patrón
  que usaría un cluster Spark real en producción: la config de jars/extensiones
  vive en `spark-defaults.conf`, no hardcodeada en cada script.
- `profiles.yml.example` — copiar a `profiles.yml` (gitignoreado, mismo patrón
  que `.env`/`.env.example`).

## Setup (una sola vez)

```bash
pip install -r transformation/requirements.txt
cp transformation/profiles.yml.example transformation/profiles.yml
```

## Variables de entorno necesarias antes de correr dbt

```bash
export SPARK_CONF_DIR="/c/Users/blanc/dev/smart-energy-pipeline/smart-energy-pipeline/transformation"
export DBT_PROFILES_DIR="/c/Users/blanc/dev/smart-energy-pipeline/smart-energy-pipeline/transformation"
export LAKEHOUSE_BASE_PATH="/c/Users/blanc/dev/smart-energy-pipeline/smart-energy-pipeline/streaming/lakehouse"
```

Ojo con las barras: son rutas que Spark arma como URI internamente, así que
van con `/`, no con `\`, aunque estés en Git Bash sobre Windows. Ajustá el
prefijo de disco (`/c/...`) a tu ruta real si cambia.

## Verificar la conexión

```bash
cd transformation/dbt_project
dbt debug
```

Esto no corre ningún modelo todavía — solo confirma que dbt puede levantar la
SparkSession con Delta configurado. Es el primer punto real de fricción
esperable en esta fase (Windows + JVM + classpath, mismo tipo de problema que
tuvimos con Spark en Fase 2) — si tira error, pegame el output completo antes
de seguir.
