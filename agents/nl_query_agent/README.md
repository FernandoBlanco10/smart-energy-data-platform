# Agente NL→SQL (`agents/nl_query_agent/`)

Prototipo del Agente 2 (`docs/agent-layer-spec.md`): recibe una pregunta en
lenguaje natural sobre la capa Gold y devuelve resultado tabular +
explicación en texto, usando la Claude API con tool-use.

## Por qué DuckDB acá (y no Spark/dbt-spark)

Gold ya quedó escrita por dbt como Parquet nativo (más `dim_city` en Delta,
ver ADR-010) — este agente solo **lee** lo que ya está ahí, nunca escribe.
No hace falta levantar una JVM/SparkSession para eso: DuckDB lee Parquet
nativamente y Delta con su propia extensión, arranca casi instantáneo, y
sirve mejor el guardrail central de la spec (conexión `read_only=True` a
nivel de motor, ver `agent.py`). Ver ADR-011 para el detalle completo,
incluyendo por qué esto no contradice ADR-009 (que descartó DuckDB para la
capa de *transformación*, un problema distinto — ahí importaba escribir
Delta, acá solo importa leer).

## Setup

```bash
cd agents/nl_query_agent
pip install -r requirements.txt
```

Completá en `.env` (raíz del proyecto):

```
ANTHROPIC_API_KEY=tu-key-de-https://console.anthropic.com/settings/keys
```

## Uso

1. Corré `dbt build` en `transformation/dbt_project/` (ver
   `transformation/README.md`) al menos una vez, para que Gold exista.
2. Construí el catálogo de consulta:
   ```bash
   python build_gold_catalog.py
   ```
   Esto crea `transformation/gold.duckdb` con vistas de solo lectura sobre
   las tablas Gold. Volvé a correrlo si aparece o desaparece una tabla
   entera — si solo cambiaron los datos (otro `dbt build`), no hace falta,
   las vistas se re-leen solas.
3. Preguntá:
   ```bash
   python agent.py "¿cuál es el consumo promedio a las 18hs?"
   ```
   o sin argumentos para modo interactivo (`python agent.py`).

## Guardrails (ver docs/agent-layer-spec.md, Agente 2)

- El agente **nunca** toca Bronze ni Silver — solo conoce las tablas de
  `gold.duckdb`.
- Toda consulta se valida como `SELECT`-only en código (`validate_select_only`
  en `agent.py`) **y además** se ejecuta contra una conexión DuckDB abierta
  en `read_only=True` — la restricción real es la del motor, no el chequeo
  de código ni el prompt.
- Loop de tool-use con tope duro de `MAX_TOOL_ITERATIONS` — no reintenta
  indefinido.
- Cada pregunta y el SQL que generó quedan logueados en `logs/` (auditable).

## Qué falta para producción (fuera de alcance del prototipo)

- Un usuario de base de datos real con permisos `SELECT`-only a nivel de
  motor (acá el equivalente es `read_only=True` de DuckDB — válido para el
  prototipo local, pero un motor multiusuario real usaría roles/grants).
- Servir esto como API en vez de CLI (Fase 4, dashboard).
