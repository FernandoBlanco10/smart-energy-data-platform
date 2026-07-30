# Project Brief: Smart Energy & Weather Pipeline

## 1. Visión general

Plataforma de ingeniería de datos distribuida y agnóstica a la nube para la optimización de redes eléctricas inteligentes (Smart Grid). El sistema integra dos flujos:

1. **Streaming (tiempo real):** variables meteorológicas (temperatura, humedad, viento) vía API de OpenWeatherMap para la región Este de EE. UU.
2. **Batch/simulación:** consumo eléctrico histórico en MW del operador PJM East Region (PJME, 2001-2018), reproducido como streaming.

**Objetivo técnico final:** procesar ambos flujos en un Data Lakehouse (Delta Lake), transformarlos con dbt, entrenar un modelo predictivo de demanda eléctrica, exponer resultados en un dashboard — y sostener todo esto con una capa de agentes de IA que monitorea, explica y responde consultas sobre el sistema.

**Objetivo de aprendizaje:** dominar el ciclo completo de un pipeline de datos moderno (ingesta → streaming → lakehouse → transformación → ML → orquestación → agentes) y ser capaz de justificar cada decisión de diseño, no solo ejecutarla.

## 2. Stack tecnológico (resumen — justificación completa en `docs/architecture-decisions.md`)

| Componente | Tecnología |
|---|---|
| IaC | Terraform |
| Contenedores | Docker & Docker Compose |
| Ingesta streaming | Apache Kafka |
| Procesamiento distribuido | PySpark (Spark Structured Streaming) |
| Almacenamiento | Delta Lake (Bronze / Silver / Gold) |
| Transformación | dbt |
| Orquestación | Apache Airflow |
| ML | Python (Scikit-Learn / XGBoost) + MLflow |
| Calidad de datos | dbt tests + dbt-expectations |
| CI/CD | GitHub Actions |
| Agentes de IA | LLM con tool-use (Claude API) sobre Airflow, dbt y capa Gold |

## 3. Especificaciones de ingesta de datos

### A. Datos meteorológicos (OpenWeatherMap API)

- **Frecuencia:** GET cada 2 minutos.
- **Ubicaciones (región PJME):** Philadelphia (PA), Newark (NJ), Baltimore (MD), Wilmington (DE), Washington (DC).
- **Tópico Kafka:** `weather-stream`
- **Esquema JSON:**
  ```json
  {
    "timestamp": "YYYY-MM-DD HH:MM:SS",
    "city": "String",
    "country": "US",
    "temperature_celsius": 0.0,
    "humidity_percentage": 0,
    "wind_speed_m_s": 0.0,
    "condition": "String"
  }
  ```

### B. Datos históricos de red eléctrica (PJME_hourly.csv)

- **Estrategia:** el productor Python lee el CSV secuencialmente y envía 1 fila por segundo (simula 1 hora de consumo real).
- **Tópico Kafka:** `energy-stream`
- **Esquema JSON:**
  ```json
  {
    "timestamp": "YYYY-MM-DD HH:MM:SS",
    "grid_region": "PJME",
    "consumption_mw": 0.0
  }
  ```

## 4. Reglas generales para el agente de desarrollo (Claude Code u otro)

- **No inventar código no solicitado.** Toda propuesta debe responder a la fase actual documentada en `.context/roadmap.md`.
- **Manejo de excepciones obligatorio.** Todo código Python debe tener `try/except` y logging (no `print`).
- **Código limpio en SQL.** Los modelos dbt siguen las convenciones del proyecto: palabras clave en MAYÚSCULAS, uso de CTEs, nombres de modelo `stg_`, `int_`, `fct_`/`dim_` según capa.
- **Secretos nunca en el código.** API keys y credenciales van en `.env` (gitignored) o en Airflow Connections — nunca hardcodeadas.
- **Toda decisión de arquitectura que se tome o cambie durante el desarrollo se documenta** en `docs/architecture-decisions.md` con el formato ADR ya establecido.
