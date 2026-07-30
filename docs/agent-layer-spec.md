# Especificación de la capa de agentes de IA

Este documento define **qué hace cada agente, con qué herramientas cuenta, y qué no puede hacer.** Es el documento que más se te va a preguntar en una defensa, porque es la parte menos "de manual" del proyecto.

## Principio de diseño general

Un agente en este pipeline nunca es la primera línea de defensa. Las reglas deterministas (dbt tests, umbrales, reintentos automáticos de Airflow) actúan primero porque son más baratas, más rápidas y 100% predecibles. El LLM entra cuando la tarea requiere **razonamiento sobre contexto ambiguo**: diagnosticar por qué falló algo, traducir una pregunta humana a SQL, o explicar una anomalía cruzando variables que una regla fija no relaciona fácilmente.

## Agente 1 — Monitor y triage (`agents/monitor_agent/`)

- **Entrada:** logs de tareas de Airflow, resultados de `dbt test`, métricas de lag de consumidores de Kafka.
- **Herramientas (tool-use):** lectura de logs vía Airflow REST API, lectura de `run_results.json` de dbt, capacidad de reintentar una tarea de Airflow (`trigger_dag_run` con permisos acotados), capacidad de enviar una alerta (Slack/email).
- **Qué NO puede hacer:** modificar código, modificar esquemas, borrar datos, reintentar más de N veces sin escalar a un humano.
- **Criterio de escalamiento:** si el mismo fallo se repite tras un reintento automático, o si el fallo involucra pérdida/corrupción de datos (no solo timeout), el agente escala a un humano en vez de seguir reintentando — esto se documenta explícitamente porque es la pregunta típica de defensa ("¿y si el agente reintenta infinito?").

## Agente 2 — Consulta en lenguaje natural (`agents/nl_query_agent/`)

- **Entrada:** pregunta en lenguaje natural sobre la capa Gold (ej. "¿cuál fue el consumo pico en Baltimore la semana pasada?").
- **Herramientas:** generación de SQL restringido a `SELECT` sobre vistas/tablas Gold predefinidas (nunca sobre Bronze/Silver directamente), ejecución contra un usuario de base de datos de solo lectura.
- **Guardrail central:** el usuario de base de datos que usa este agente tiene permisos `SELECT`-only a nivel de motor de datos — no es una instrucción en el prompt, es una restricción real de credenciales. Esto es intencional: un guardrail de prompt se puede eludir, un permiso de base de datos no.
- **Salida:** resultado tabular + explicación en texto de qué significa el resultado (no solo el número crudo).

## Agente 3 — Detección de anomalías contextual (`agents/anomaly_agent/`, Fase 5)

- **Entrada:** series de consumo (Gold) + series climáticas (Gold) de la misma ventana temporal.
- **Lógica:** un detector estadístico simple (z-score o IQR) marca el candidato a anomalía primero; el LLM entra después, solo sobre los candidatos ya marcados, para cruzar la anomalía con el contexto climático y generar una explicación legible.
- **Por qué no todo el detection es LLM:** correr un LLM sobre cada punto de una serie temporal completa es costoso e innecesario — la estadística ya resuelve la detección; el LLM aporta valor en la *interpretación*, no en el cálculo.

## Agente 4 — Auto-documentación (`agents/docs_agent/`, Fase 5)

- **Entrada:** diffs de esquema en los modelos dbt.
- **Salida:** propuesta de actualización de `schema.yml` / descripciones de columnas — **siempre como propuesta a revisar en un PR, nunca como commit automático directo a main.**

## Guardrails transversales (aplican a los cuatro agentes)

1. Ningún agente tiene credenciales de escritura sobre Bronze o Silver.
2. Toda acción de un agente queda logueada con el prompt que la originó (auditabilidad).
3. Ningún agente reintenta o actúa de forma indefinida sin límite de intentos.
4. Los agentes de escritura (docs, remediación) siempre producen un artefacto para revisión humana, no un cambio directo a producción.
