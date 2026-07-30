# Roadmap de ejecución

Cada fase tiene: objetivo, entregables, y la pregunta de defensa que deberías poder responder al cerrarla (ver también `docs/defense-qa.md`).

---

## Fase 1 — Infraestructura y entorno local (Semanas 1-2)

**Objetivo:** tener un entorno reproducible donde cualquiera pueda levantar el proyecto con un comando.

**Entregables:**
- Estructura del proyecto y `.context/` (este repo).
- `docker-compose.yml` con Kafka + Zookeeper + Airflow.
- Aprovisionamiento base en Terraform (`infra/terraform/`) — buckets S3/Blob, sin desplegar aún a producción.
- Productor Python para OpenWeatherMap (`ingestion/weather_producer.py`) con manejo de errores y logging.

**Pregunta de defensa:** ¿por qué Docker Compose y no simplemente instalar Kafka y Airflow localmente?

---

## Fase 2 — Streaming, Lakehouse e ingesta batch (Semanas 3-6)

**Objetivo:** tener datos fluyendo de extremo a extremo, desde las fuentes hasta Delta Lake, con calidad verificada.

**Entregables:**
- Simulador de streaming para `PJME_hourly.csv` (`ingestion/energy_producer.py`).
- Pipeline Spark Structured Streaming que consume ambos tópicos Kafka.
- Capa **Bronze** (datos crudos, append-only) y **Silver** (datos limpios, deduplicados, tipados) en Delta Lake.
- Tests de calidad de datos en Silver (no nulos, rangos válidos, unicidad de timestamp) — **obligatorio, no opcional.**

**Pregunta de defensa:** ¿qué garantiza Delta Lake que un simple Parquet no garantiza, y por qué importa acá?

---

## Fase 3 — Transformación con dbt y prototipo del agente de consulta (Semanas 7-8)

**Objetivo:** capa Gold lista para analítica, y primer agente de IA funcionando sobre ella.

**Entregables:**
- Proyecto dbt inicializado en `transformation/dbt_project/`.
- Modelos con CTEs y funciones de ventana (`LAG`, `LEAD`, `AVG() OVER`) para unir clima y consumo por ventana temporal.
- Capa **Gold** (data marts) documentada con `dbt docs`.
- Prototipo del **agente NL→SQL** (`agents/nl_query_agent/`): recibe una pregunta en español/inglés, genera SQL de solo lectura contra Gold, devuelve resultado + explicación.

**Pregunta de defensa:** ¿por qué mover la lógica de transformación a dbt (ELT) en vez de transformar en Spark antes de guardar (ETT)?

---

## Fase 4 — Modelo predictivo, dashboard, agente de monitoreo y CI/CD (Semanas 9-12)

**Objetivo:** cerrar el ciclo con predicción, visualización, observabilidad automatizada y despliegue continuo.

**Entregables:**
- Modelo de regresión/series temporales (`ml/`) entrenado sobre Gold, tracking con MLflow.
- Dashboard conectado a Gold (Streamlit/Power BI/Looker Studio).
- **Agente de monitoreo/triage** (`agents/monitor_agent/`): lee logs de Airflow y resultados de dbt tests, diagnostica fallas y alerta.
- Pipeline de GitHub Actions: lint de Python, validación de sintaxis dbt, tests antes de merge.

**Pregunta de defensa:** ¿cómo decide el agente de monitoreo cuándo alertar a un humano vs. cuándo reintentar automáticamente?

---

## Fase 5 (opcional) — Agente de anomalías y auto-documentación (Semana 13+)

**Objetivo:** cerrar la capa de agentes con detección contextual y mantenimiento automático de documentación.

**Entregables:**
- Agente que cruza picos de consumo anómalo con condiciones climáticas y genera una explicación en texto.
- Agente que actualiza `dbt docs` y comentarios de esquema cuando cambian los modelos.

**Pregunta de defensa:** ¿por qué un LLM aporta algo que un umbral estadístico (z-score) no aporta solo?
