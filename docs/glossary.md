# Glosario para defender el proyecto

Explicado con tus propias palabras es mejor, pero acá tenés la base para no quedarte en blanco.

**Lakehouse** — arquitectura que combina la flexibilidad y el bajo costo de un data lake (almacenamiento de objetos, cualquier formato) con las garantías transaccionales de un data warehouse (ACID, schema enforcement). Delta Lake es la capa que hace esto posible sobre S3/Blob Storage.

**ACID** — Atomicidad, Consistencia, Aislamiento, Durabilidad. Garantiza que una escritura sobre una tabla Delta o se completa entera o no se aplica nada — nunca queda a medias, incluso si el job de Spark se cae en el medio.

**Time travel** — capacidad de Delta Lake de consultar una tabla como estaba en una versión o timestamp anterior, útil para debugging y para auditar cambios.

**Arquitectura medallion (Bronze/Silver/Gold)** — patrón de organización por capas de calidad creciente: Bronze son datos crudos tal cual llegan (append-only, sin transformar), Silver son datos limpios y tipados (deduplicados, validados), Gold son datos agregados y listos para consumo de negocio o ML.

**ELT vs ETL** — en ETL transformás los datos antes de cargarlos al destino; en ELT cargás los datos crudos primero y transformás dentro del motor de datos (acá: dbt sobre Delta Lake). ELT es el patrón moderno porque aprovecha el poder de cómputo del motor de datos y deja el historial crudo disponible para reprocesar.

**CTE (Common Table Expression)** — un `WITH nombre AS (...)` en SQL; permite dividir una consulta compleja en pasos nombrados y legibles, en vez de subconsultas anidadas.

**Función de ventana (window function)** — `AVG(consumo) OVER (PARTITION BY ciudad ORDER BY fecha)` calcula un agregado sin colapsar filas, lo que permite comparar cada fila con su contexto (por ejemplo, la hora anterior con `LAG`).

**DAG (Directed Acyclic Graph)** — grafo de tareas sin ciclos que define en qué orden se ejecutan los jobs. Airflow usa DAGs para orquestar: cada nodo es una tarea, cada arista es una dependencia.

**Backpressure / consumer lag (Kafka)** — cuando el consumidor (Spark) procesa más lento de lo que el productor escribe al tópico, se acumula un retraso (lag). Monitorear el lag es clave para saber si el pipeline va a tiempo real o se está atrasando.

**Tool-use / function calling (LLM)** — capacidad de un modelo de lenguaje de invocar funciones externas definidas por vos (consultar una API, ejecutar SQL) en vez de solo generar texto. Es lo que convierte a un LLM de "chatbot" a "agente" dentro de este pipeline.

**Guardrail** — restricción que limita lo que un agente puede hacer, idealmente aplicada a nivel de sistema (permisos de base de datos, límites de API) y no solo a nivel de instrucción en el prompt, porque las instrucciones en el prompt se pueden eludir y los permisos del sistema no.
