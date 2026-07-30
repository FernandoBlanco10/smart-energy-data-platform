# Preguntas esperadas en una defensa técnica

Respuestas guía — adaptalas con tu propia experiencia real del proyecto (lo que anotaste en `.context/learning-log.md` es tu mejor fuente).

**¿Por qué Kafka y no llamar directamente a la API desde Spark?**
Porque desacopla la disponibilidad del pipeline de la disponibilidad de la fuente externa. Si OpenWeatherMap tiene un rate limit o cae, Kafka retiene los eventos ya ingeridos y Spark sigue procesando sin perder datos ni caerse junto con la fuente.

**¿Qué pasa si dos jobs de Spark escriben en la misma tabla Delta al mismo tiempo?**
Delta Lake usa control de concurrencia optimista con logs de transacciones — una de las escrituras se aplica atómicamente y la otra falla y reintenta, en vez de corromper la tabla a medio escribir. Esto es justamente lo que Parquet plano no garantiza.

**¿Por qué separar Silver y Gold si ambos están "limpios"?**
Silver es limpio a nivel de fila (tipos correctos, sin nulos donde no debería haberlos, deduplicado) pero sigue siendo granular y neutral respecto al negocio. Gold ya está agregado y modelado para una pregunta de negocio específica (ej. consumo horario por ciudad con su clima asociado) — es lo que consume el dashboard o el modelo de ML directamente.

**¿Por qué dbt y no simplemente vistas SQL en el motor de datos?**
dbt aporta versionado en Git, tests automatizados por modelo, documentación generada y linaje de datos (qué modelo depende de cuál). Una vista SQL suelta no tiene nada de eso — si se rompe, no hay forma sistemática de saber qué la afectó.

**¿Por qué necesitás un agente de IA si ya tenés dashboards?**
El dashboard responde las preguntas que anticipaste al diseñarlo. El agente NL→SQL responde preguntas ad-hoc que nadie anticipó, sin que tengas que escribir SQL cada vez ni pedirle a un ingeniero de datos que lo haga. No reemplaza el dashboard — cubre el espacio que el dashboard no cubre.

**¿Cómo evitás que el agente de consulta borre o modifique datos?**
El guardrail no es una instrucción en el prompt — es una restricción real: el usuario de base de datos que usa el agente tiene permisos `SELECT`-only a nivel de motor. Aunque alguien lograra manipular el prompt para pedir un `DELETE`, la base de datos lo rechazaría antes de ejecutarlo.

**¿Por qué no usar el LLM para todo el pipeline, incluida la detección de anomalías fila por fila?**
Por costo y porque no es el problema correcto: la detección estadística (z-score/IQR) es determinista, barata y suficiente para marcar candidatos. El LLM se reserva para lo que la estadística no puede hacer bien: interpretar *por qué* algo es anómalo cruzando variables de contexto.

**¿Qué harías distinto si tuvieras que escalar esto a producción real con más regiones?**
Punto abierto para responder con tu propio criterio una vez que hayas pasado por las fases — pensalo en términos de: particionado de Kafka por región, coste de Databricks a escala, y si Airflow sigue siendo suficiente o hace falta algo como Dagster con mejor manejo de assets de datos.
