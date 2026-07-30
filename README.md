# Smart Energy & Weather Pipeline

Plataforma de ingeniería de datos distribuida y agnóstica a la nube para la optimización de redes eléctricas inteligentes (Smart Grid), con una capa de agentes de IA integrada para monitoreo, consulta en lenguaje natural y detección de anomalías.

## Por qué existe este repositorio

Este proyecto es a la vez una plataforma funcional y un ejercicio de aprendizaje deliberado. Cada carpeta de `.context/` y `docs/` documenta **qué se construyó y por qué**, para que el proyecto pueda:

1. Servir como plataforma real de portafolio (Data Engineering + IA aplicada).
2. Ser defendido técnicamente — cada decisión de arquitectura tiene su justificación y sus alternativas descartadas.
3. Funcionar como bitácora de aprendizaje incremental, no solo como código final.

## Mapa del repositorio

```
smart-energy-pipeline/
├── .context/                  # Fuente de verdad para vos y para el agente de desarrollo (Claude Code)
│   ├── project-brief.md       # Visión, alcance, esquemas de datos
│   ├── roadmap.md             # Fases de ejecución, con la capa de agentes integrada
│   └── learning-log.md        # Bitácora de aprendizaje por fase (la llenás vos)
├── docs/
│   ├── architecture-decisions.md  # ADRs: por qué cada tecnología, qué se descartó
│   ├── agent-layer-spec.md        # Diseño de los agentes de IA como componente de producción
│   ├── glossary.md                # Conceptos clave explicados para defender el proyecto
│   └── defense-qa.md              # Preguntas esperadas en una defensa técnica, con respuestas
├── infra/terraform/           # IaC
├── ingestion/                  # Productores Python (Kafka)
├── streaming/                  # Jobs de Spark Structured Streaming
├── transformation/dbt_project/ # Modelos dbt (Silver → Gold)
├── ml/                         # Modelo predictivo de demanda
└── agents/
    ├── monitor_agent/          # Agente de monitoreo/triage del pipeline
    └── nl_query_agent/         # Agente de consulta en lenguaje natural sobre la capa Gold
```

## Cómo usar esta documentación mientras desarrollás

1. Antes de tocar código en una fase nueva, releé `.context/roadmap.md` para esa fase.
2. Cuando tomes o cuestiones una decisión de arquitectura, anotala en `docs/architecture-decisions.md` con el mismo formato ADR — así el archivo crece con vos, no queda estático.
3. Al cerrar cada fase, escribí 3-5 líneas en `.context/learning-log.md`: qué entendiste que antes no, qué error cometiste, qué preguntarías en una entrevista sobre esa fase.
4. Antes de defender el proyecto, repasá `docs/defense-qa.md` y `docs/glossary.md`.

## Flujo de Git

- **Commits:** [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`. Facilita generar changelog y es el estándar que vas a encontrar en la mayoría de repos profesionales.
- **Ramas:** trunk-based. `main` siempre desplegable; una rama corta por tarea, `feat/fase1-docker-compose`, `feat/fase2-spark-bronze-silver`, etc. Merge a `main` vía Pull Request, aunque trabajes solo — deja rastro de revisión y es donde engancha el CI de GitHub Actions (Fase 4).
- **Tags de fase:** al cerrar cada fase del roadmap, tag `v0.1-fase1`, `v0.2-fase2`, etc. — le da al repo una narrativa de portafolio clara.
- **Nunca commitear:** `.env`, `terraform.tfstate`, credenciales. Ver `.gitignore`. En GitHub, activá *secret scanning* + *push protection* en Settings → Security (gratis, incluso en repos privados).
