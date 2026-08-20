# Architecture Decision Records (ADRs)

Cada entrada sigue el mismo formato: **Contexto → Decisión → Alternativas descartadas → Consecuencias.** El objetivo de este archivo es que puedas defender cualquier elección tecnológica del proyecto sin depender de la memoria — la razón siempre está escrita acá.

---

## ADR-001: Terraform como herramienta de IaC

**Contexto:** el proyecto debe ser agnóstico a la nube y reproducible sin configuración manual.

**Decisión:** usar Terraform para aprovisionar buckets (S3/Azure Blob) y clústeres.

**Alternativas descartadas:**
- *Configuración manual vía consola web* — descartada por no ser reproducible ni versionable.
- *CloudFormation (AWS)* — descartada porque ata el proyecto a un solo proveedor, contradiciendo el objetivo de agnosticismo.
- *Pulumi* — válida técnicamente, pero Terraform tiene mayor adopción en la industria y más ejemplos para Kafka/Databricks, lo cual pesa en un proyecto de aprendizaje.

**Consecuencias:** curva de aprendizaje de HCL; a cambio, todo el estado de infraestructura queda versionado en Git y es auditable.

---

## ADR-002: Docker & Docker Compose para el entorno local

**Contexto:** Kafka y Airflow tienen dependencias complejas (Zookeeper, bases de metadatos, brokers) que varían según el sistema operativo del desarrollador.

**Decisión:** aislar todo el entorno local con Docker Compose.

**Alternativas descartadas:**
- *Instalación nativa de Kafka/Airflow* — descartada: genera "funciona en mi máquina", especialmente entre Windows/Mac/Linux.
- *Kubernetes local (minikube/kind) desde el inicio* — descartado por sobre-ingeniería en fase 1; Kubernetes tiene sentido cuando el proyecto necesite escalar horizontalmente, no para desarrollo individual.

**Consecuencias:** un solo `docker-compose up` levanta el entorno completo; el costo es la curva de aprendizaje de Docker networking entre contenedores.

---

## ADR-003: Apache Kafka para ingesta de streaming

**Contexto:** hay dos fuentes de datos heterogéneas (API REST y CSV simulado) que deben desacoplarse del motor de procesamiento.

**Decisión:** Kafka como broker de mensajería, con un tópico por fuente.

**Alternativas descartadas:**
- *Llamar directamente a Spark sin broker intermedio* — descartada: acopla la disponibilidad del pipeline a la disponibilidad de la API externa; si OpenWeatherMap cae, Spark no debería caerse también.
- *AWS Kinesis / Azure Event Hubs* — descartados por atar el proyecto a un proveedor de nube específico (mismo argumento que ADR-001).
- *RabbitMQ* — válido para colas de tareas, pero Kafka está diseñado específicamente para streams de eventos de alto volumen con retención y replay, que es lo que este proyecto necesita.

**Consecuencias:** tolerancia a fallos y desacoplamiento real; a cambio, hay que operar y monitorear un broker adicional.

---

## ADR-003b: KRaft en vez de Zookeeper para coordinación de Kafka

**Fecha:** 2026-07-29. **Actualiza:** el diseño original (roadmap Fase 1) asumía Kafka + Zookeeper.

**Contexto:** Zookeeper era el mecanismo original de Kafka para almacenar metadata del clúster, elegir el broker líder y coordinar controladores. Desde Kafka 3.3 (KIP-500) KRaft es production-ready, y **desde Kafka 4.0 (2024) Zookeeper fue eliminado del proyecto por completo** — ya no es una opción, es el modo por defecto en cualquier instalación actual.

**Decisión:** usar Kafka en modo KRaft combinado (`KAFKA_PROCESS_ROLES: broker,controller`, un solo nodo), imagen oficial `apache/kafka`, sin Zookeeper.

**Alternativas descartadas:**
- *Mantener Zookeeper (Confluent cp-kafka + cp-zookeeper)* — descartado: hoy describiría una arquitectura obsoleta en una entrevista técnica. La versión estable actual de Kafka (4.3.x) ni siquiera lo soporta.
- *KRaft en modo aislado (controladores separados de brokers, como en un clúster de 3+ nodos)* — es el patrón de producción real, pero sobre-ingeniería para un solo desarrollador en local; se documenta acá para poder explicarlo en la defensa aunque no se implemente.

**Consecuencias:** un contenedor menos que operar y un punto de fallo menos; a cambio, hay que poder explicar en la defensa qué hacía Zookeeper (elección de líder, almacenamiento de metadata, ACLs) y por qué Kafka ahora lo resuelve internamente con un quórum Raft entre los propios brokers/controladores (`KAFKA_CONTROLLER_QUORUM_VOTERS`).

---

## ADR-004: PySpark (Structured Streaming) sobre Databricks

**Contexto:** se necesita procesar dos streams estructurados con operaciones de *windowing* y unión temporal.

**Decisión:** Spark Structured Streaming, con Databricks como plataforma de ejecución cuando el proyecto lo justifique.

**Alternativas descartadas:**
- *Flink* — mejor latencia en streaming puro, pero el ecosistema de Delta Lake y dbt está más maduro en Spark, y el proyecto ya requiere aprender Spark para la parte batch.
- *Pandas/Python puro para el procesamiento* — descartado: no escala a volúmenes reales ni tiene soporte nativo de *windowing* sobre streams.

**Nota práctica:** para desarrollo y aprendizaje, considerá Spark local en Docker (`bitnami/spark`) o Databricks Community Edition antes de pagar por un workspace completo — Databricks pago se justifica recién cuando necesites demostrar el despliegue en un entorno gestionado real.

**Consecuencias:** procesamiento distribuido real; el costo es la complejidad operativa y, si se usa Databricks pago, el costo económico.

---

## ADR-005: Delta Lake como formato de almacenamiento

**Contexto:** se necesitan transacciones ACID sobre object storage y una separación clara entre datos crudos, limpios y de negocio.

**Decisión:** Delta Lake con arquitectura medallion (Bronze/Silver/Gold).

**Alternativas descartadas:**
- *Parquet plano* — descartado: no soporta ACID, así que una escritura concurrente o fallida puede corromper una tabla completa; tampoco tiene *time travel*.
- *Data warehouse tradicional (Snowflake/BigQuery) desde el inicio* — descartado por costo y por atar el proyecto a un proveedor; además el objetivo es demostrar competencia en Lakehouse, no en warehousing gestionado.

**Consecuencias:** se gana ACID, time travel y schema enforcement; se paga con overhead de metadata comparado con Parquet plano.

---

## ADR-006: dbt para transformación (ELT en vez de ETL)

**Contexto:** las transformaciones de negocio (unión temporal clima-consumo, agregaciones) deben ser versionables, testeables y documentadas.

**Decisión:** dbt sobre la capa Silver, generando Gold, en vez de transformar todo dentro del job de Spark.

**Alternativas descartadas:**
- *Transformar todo en PySpark antes de persistir* — descartado: mezcla lógica de ingeniería (streaming, particionado) con lógica de negocio (reglas analíticas), dificultando el mantenimiento y el testing independiente de cada capa.
- *Stored procedures SQL sin dbt* — descartado: no da versionado, tests automatizados ni linaje de datos out-of-the-box.

**Consecuencias:** separación limpia entre "cómo llegan los datos" (Spark) y "qué significan los datos para el negocio" (dbt); el costo es una capa adicional de orquestación que Airflow debe coordinar.

---

## ADR-007: Apache Airflow para orquestación

**Contexto:** hay múltiples jobs interdependientes (productores, Spark, dbt) que deben ejecutarse en orden y con reintentos.

**Decisión:** Airflow con DAGs explícitos.

**Alternativas descartadas:**
- *Cron jobs simples* — descartado: no maneja dependencias entre tareas, reintentos ni observabilidad de fallos.
- *Prefect / Dagster* — alternativas modernas válidas, con mejor manejo de datos como ciudadanos de primera clase (Dagster especialmente). Se descartaron por menor adopción en ofertas de trabajo actuales, lo cual importa para el objetivo de portafolio.

**Consecuencias:** Airflow es la pieza que conecta con el agente de monitoreo (Fase 4) — sus logs y metadata de ejecución son la fuente de verdad para que el agente diagnostique fallas.

---

## ADR-008: LLM con tool-use como capa de agentes (no solo copiloto de código)

**Contexto:** un pipeline de datos tradicional requiere revisión manual de logs, escritura manual de SQL ad-hoc para analistas, y documentación que se desactualiza.

**Decisión:** construir agentes de IA como componentes de producción — con acceso restringido y auditable a Airflow, dbt y la capa Gold — en vez de usar IA únicamente como asistente durante el desarrollo.

**Alternativas descartadas:**
- *Solo usar IA para generar código durante el desarrollo* — es útil, pero no diferencia el perfil de "Data Engineer" del de "AI Data Engineer"; no demuestra diseño de sistemas con LLMs como componente.
- *Dashboards de BI tradicionales sin capa conversacional* — siguen siendo necesarios (Fase 4), pero no reemplazan la exploración ad-hoc que resuelve el agente NL→SQL.
- *Reglas fijas (if/else, umbrales estadísticos) para monitoreo* — siguen usándose como primera línea (son más baratas y deterministas); el agente LLM se reserva para diagnóstico y explicación cuando la regla fija no alcanza a explicar la causa.

**Consecuencias:** más superficie de riesgo a gestionar (ver `docs/agent-layer-spec.md` para guardrails: el agente NL→SQL solo puede ejecutar `SELECT`, nunca DDL/DML); a cambio, el sistema se vuelve auto-explicativo y consultable en lenguaje natural.

---

## ADR-009: dbt-spark (modo `session`) como adaptador de dbt sobre Delta Lake

**Fecha:** 2026-08-09. **Contexto de ADR-006:** ya se había decidido usar dbt sobre Silver para generar Gold, pero no se había definido con qué motor — dbt no lee archivos Delta directamente, necesita un adaptador que hable con algún engine de consulta.

**Contexto:** Silver ya vive en Delta Lake, escrito y leído hasta ahora exclusivamente con PySpark local (Spark 3.5.8 + Delta 3.3.2, ver ADR-004/005). El objetivo del proyecto exige seguir sin gastar en nube ni en un cluster gestionado (Databricks pago).

**Decisión:** `dbt-spark` con `method: session` en `profiles.yml` — dbt levanta una SparkSession embebida dentro de su propio proceso Python (sin Thrift server, sin cluster externo), reutilizando exactamente la misma configuración de Delta (paquetes Ivy, extensiones) que ya se armó en `streaming/common/spark_session.py` para Bronze/Silver.

**Alternativas descartadas:**
- *dbt-duckdb + extensión Delta* — motor más liviano (sin JVM, arranque casi instantáneo), cada vez más adoptado en la industria para analítica local. Se descartó para esta fase porque el soporte de **escritura** Delta desde DuckDB es menos maduro que su lectura — Gold hubiera quedado en un formato distinto al de Bronze/Silver, rompiendo la consistencia "todo el lakehouse es Delta". Queda anotado como alternativa válida a reconsiderar si el proyecto necesitara iteración más rápida en el futuro.
- *dbt-spark con Thrift server (`method: thrift`)* — es el patrón real de un cluster Spark compartido en producción (varios clientes conectándose a un mismo cluster de larga duración), pero es sobre-ingeniería para un solo desarrollador local: exige levantar y mantener un servicio adicional sin beneficio real a esta escala.
- *Databricks (Community Edition o pago)* — descartado por el mismo argumento que ADR-004: válido para demostrar despliegue gestionado más adelante, pero no es necesario para construir y defender la lógica de transformación en sí.

**Consecuencias:** cero herramientas nuevas en el stack, y el flujo de trabajo (dbt hablándole a una SparkSession local) es el mismo patrón que usaría un equipo con Databricks/EMR en producción, solo que sin el costo — se puede migrar a `method: thrift` u otro cluster real cambiando únicamente `profiles.yml`, sin tocar los modelos SQL. El costo es el arranque de JVM en cada corrida de `dbt run` (varios segundos) y tener que repetir la config de Delta (`spark.jars.packages`) también en el profile de dbt, no solo en `spark_session.py`.

---

## ADR-010: Gold en Parquet, no en Delta

**Fecha:** 2026-08-13. **Contexto:** intentando materializar `models/gold/` como tablas Delta (consistente con Bronze/Silver), toda corrida de `dbt build` falló igual, con y sin `location_root`, con tabla nueva y con tabla ya existente: `org.apache.spark.sql.AnalysisException: Table ... does not support truncate in batch mode`.

**Investigación:** el stack trace completo (`AtomicReplaceTableAsSelectExec` → `TableCapabilityCheck`) muestra la causa real: la materialización `table` de dbt-spark reconstruye siempre con `CREATE OR REPLACE TABLE ... AS SELECT` para formatos como Delta. Esa sentencia pasa por el framework genérico DataSourceV2 de Spark para reemplazo atómico, que exige que la tabla declare la capacidad de truncar en lote (`TableCapability.TRUNCATE`). La implementación de Delta usada acá (Delta 3.3.2 sobre Spark 3.5.8, vía `spark_catalog` overridden como `DeltaCatalog`) no expone esa capacidad por este camino — se reprodujo el error de forma idéntica en cuatro intentos distintos (con/sin `location_root`, catálogo limpio/con estado previo), descartando que fuera un problema de nuestra configuración.

**Decisión:** `+file_format: parquet` (Parquet nativo, no el "Hive serde" genérico por defecto) para `models/gold/`. Bronze y Silver se quedan en Delta — ahí sí importa: escrituras incrementales por streaming, posible concurrencia entre `bronze_stream.py` y `silver_stream.py` leyendo/escribiendo a la vez.

**Alternativas descartadas:**
- *Sobrescribir la macro `get_create_table_as_sql` de dbt-spark para usar `DROP TABLE` + `CREATE TABLE` en dos pasos en vez de reemplazo atómico* — mantendría Delta en Gold, pero agrega una macro custom a mantener para un problema que Gold no necesita resolver de esa forma.
- *Materializar Gold como `incremental` en vez de `table`* — evita el `CREATE OR REPLACE`, pero cambia el modelo mental de "Gold se recalcula entera cada corrida" a upsert con `unique_key` por modelo, complejidad real no justificada a esta escala.
- *`dim_city` (seed) también se probó en Delta y funcionó sin problema* — el `INSERT` de un seed no pasa por el mismo camino de reemplazo atómico que `CREATE OR REPLACE TABLE AS SELECT`, así que se quedó en Delta; es una inconsistencia menor y consciente (un seed en Delta, las tablas de hechos en Parquet), no un descuido.

**Consecuencias:** Gold pierde ACID/time-travel — aceptable porque se reconstruye entera en cada `dbt build`, sin escrituras concurrentes, sin necesidad de auditar versiones históricas de una tabla derivada. Si algún día Gold necesitara esas garantías (ej. écrituras incrementales reales), habría que revisar esta decisión — probablemente subiendo de versión de Delta/Spark, donde este problema puede estar resuelto.

---

## ADR-011: DuckDB como motor de consulta del agente NL→SQL (no Spark/dbt-spark)

**Fecha:** 2026-08-13. **Contexto:** el Agente 2 (`agents/nl_query_agent/`, ver `docs/agent-layer-spec.md`) necesita ejecutar SQL de solo lectura contra la capa Gold. Gold ya quedó escrita por dbt como Parquet nativo (ADR-010) más `dim_city` en Delta (un seed, nunca pisó ese bug).

**Decisión:** DuckDB, apuntando con `read_parquet()`/`delta_scan()` a lo que dbt ya dejó en `transformation/dbt_project/spark-warehouse/`, expuesto como vistas en un catálogo propio (`transformation/gold.duckdb`, ver `build_gold_catalog.py`). El agente siempre abre ese catálogo con `read_only=True`.

**Por qué no reutilizar la SparkSession de dbt-spark (ADR-009):** ese camino existe y funcionaría, pero es la herramienta equivocada para este trabajo — trae una JVM entera (varios segundos de arranque por consulta) para resolver algo que es, en esencia, "leer un Parquet". Además, `method: session` de dbt-spark no da un punto de conexión reutilizable fuera del propio proceso de dbt: habría que levantar Spark de nuevo desde `agent.py`, duplicando toda la config de `spark-defaults.conf`.

**Por qué esto no contradice ADR-009 (que descartó dbt-duckdb para la capa de transformación):** son dos problemas distintos. ADR-009 necesitaba **escribir** Delta de forma consistente con Bronze/Silver — ahí el soporte de escritura de DuckDB era el punto débil. Acá el agente solo **lee** Gold, que ya ni siquiera es Delta en su mayoría (es Parquet, por ADR-010) — la lectura de Parquet con DuckDB es nativa y madura, y la lectura de Delta (para `dim_city`) usa la extensión oficial `delta`, sin necesidad de escribir nada.

**El guardrail central de la spec, resuelto con esto:** `docs/agent-layer-spec.md` exige que el permiso `SELECT`-only sea "una restricción real de credenciales", no una instrucción de prompt. `duckdb.connect(path, read_only=True)` es exactamente eso a nivel de motor de datos local — cualquier intento de escritura lo rechaza DuckDB, no el prompt ni el código del agente. Es el equivalente de un usuario de base de datos con permisos `SELECT`-only en un motor cliente-servidor real (ej. un rol de solo lectura en Postgres/Snowflake) — el prototipo usa la versión local de esa misma idea.

**Alternativas descartadas:**
- *dbt-spark `method: session` reutilizado desde el agente* — descartado por el costo de arranque de JVM y la falta de un punto de conexión reutilizable, explicado arriba.
- *Conectarse directo a los archivos Parquet/Delta desde `agent.py`, sin `gold.duckdb` de por medio* — funcionaría, pero el agente terminaría conociendo rutas de archivos internas de Spark en vez de nombres de tabla; el catálogo intermedio separa "cómo se llama la tabla" de "dónde vive físicamamente", igual que una vista en cualquier base real.

**Consecuencias:** el stack gana una herramienta nueva (DuckDB) con un rol acotado y claro (motor de consulta de solo lectura para agentes/analítica ad-hoc), consistente con la nota ya dejada en ADR-009 sobre DuckDB como "alternativa válida a reconsiderar". Arranque de agente casi instantáneo (sin JVM) y guardrail de solo-lectura real, no solo de prompt. Costo: una pieza más en el stack a explicar en la defensa, y una vista que hay que reconstruir (`build_gold_catalog.py`) si cambia el conjunto de tablas de Gold (no si solo cambian los datos).
