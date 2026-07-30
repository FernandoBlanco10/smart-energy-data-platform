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
