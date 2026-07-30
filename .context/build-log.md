# Bitácora de construcción (técnica)

Registro cronológico de qué se hizo, por qué, y con qué comandos exactos. A diferencia de `learning-log.md` (que llenás vos con tu propio entendimiento), este archivo lo mantengo yo como referencia técnica reproducible — sirve tanto para reconstruir este proyecto como plantilla para el próximo.

---

## 2026-07-29 — Fase 1: entorno local

**Contexto:** primer paso del roadmap — entorno reproducible con un comando, sin gastar en nube.

**Se creó:**
- `docker-compose.yml`: Kafka en modo **KRaft** (imagen oficial `apache/kafka:4.3.1`, nodo combinado broker+controller — ver ADR-003b), Kafka UI (`provectuslabs/kafka-ui`), Postgres 16 (metadata de Airflow), Airflow 2.9.3 (webserver + scheduler, `LocalExecutor`).
- `.env` / `.env.example`: credenciales fuera de código (regla del proyecto). `.env` tiene valores de desarrollo seguros porque el stack no sale de `localhost`.
- `.gitignore`: excluye secretos, estado de Terraform, checkpoints de Spark, datasets grandes.
- `docs/architecture-decisions.md`: ADR-003b (por qué KRaft y no Zookeeper — Kafka 4.0 eliminó Zookeeper por completo).
- `README.md`: sección "Flujo de Git" (Conventional Commits, trunk-based, tags por fase).

**Decisión revisada — Zookeeper → KRaft:** el roadmap original asumía Zookeeper. Se corrigió porque la versión estable actual de Kafka (4.3.x) ya no lo soporta. Ver ADR-003b para el detalle completo.

**Comando para levantar el stack (en tu máquina, con Docker Desktop corriendo):**
```
cd smart-energy-pipeline
docker compose up -d
```
Verificar: Airflow en `http://localhost:8080`, Kafka UI en `http://localhost:8085`.

**Incidente — control de versiones:** el proyecto vivía en una carpeta sincronizada por OneDrive (`...\Escritorio\Ingenieria de Datos\smart-energy-pipeline`). Al inicializar git ahí, las operaciones de escritura de objetos fallaban intermitentemente ("Operation not permitted" al limpiar archivos temporales) — típico de sync clients (OneDrive/Dropbox/Google Drive) reteniendo locks de archivo mientras git escribe.

Se movió el proyecto a `C:\Users\blanc\dev\smart-energy-pipeline` (fuera de cualquier carpeta sincronizada). Al reconectar la carpeta y reintentar, se confirmó algo más específico: el entorno sandbox de Cowork (donde yo ejecuto comandos de shell) **no puede eliminar archivos** en ninguna carpeta montada de Windows, sin importar si está en OneDrive o no — se probó con un archivo de prueba trivial y falló igual. Esto es una limitación del puente sandbox-Linux ↔ filesystem-Windows, no de OneDrive específicamente. Git depende fuertemente del patrón crear-lock → escribir → borrar-lock, así que cualquier `git add`/`commit` ejecutado por mí contra una carpeta montada de Windows es, en la práctica, poco confiable.

**Conclusión operativa:** de acá en adelante, yo edito archivos (Read/Write/Edit funcionan bien, son operaciones distintas), pero **los comandos de git los corrés vos**, en una terminal real de tu máquina (PowerShell, Git Bash o la integrada de VS Code). Te doy el comando exacto cada vez que hay algo para commitear.

**Pendiente (para vos, en tu terminal):**
```powershell
cd C:\Users\blanc\dev\smart-energy-pipeline\smart-energy-pipeline
Remove-Item -Recurse -Force .git      # el repo viejo quedó con locks corruptos, se descarta
git init
git add .
git commit -m "feat: Fase 1 - entorno local con Docker Compose (Kafka KRaft, Airflow, Postgres)"
```
Después, crear el repo vacío en GitHub y conectar el remoto (o esperar a que el conector de GitHub quede autorizado del lado de Claude — ver respuesta en el chat).

---

## Plantilla reutilizable para el próximo proyecto

Lo transferible de esta fase, más allá de este proyecto puntual:

1. **Nunca desarrollar con git dentro de una carpeta sincronizada (OneDrive/Dropbox/Drive).** Carpeta de trabajo local, y el remoto (GitHub) es el backup.
2. **Un `docker-compose.yml` con `x-<nombre>-common: &anchor` para servicios repetidos** (como hicimos con Airflow webserver/scheduler) evita duplicar configuración.
3. **`.env.example` versionado, `.env` real gitignorado**, siempre — incluso en proyectos de un solo desarrollador.
4. **Verificar la versión "actual" de cada herramienta antes de fijarla** (ej. Kafka KRaft vs Zookeeper) — el ecosistema de datos cambia rápido y lo que se aprendió hace 1-2 años puede estar obsoleto.
5. **ADR por cada decisión, incluso las que cambian sobre la marcha** — así el proyecto queda defendible sin depender de la memoria.
