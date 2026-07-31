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

## 2026-07-30 — Fase 1: productor de clima (`weather_producer.py`)

**Contexto:** segundo entregable de Fase 1 — llevar datos reales de OpenWeatherMap a Kafka.

**Se creó:**
- `ingestion/common/logging_config.py`: logger compartido (consola + archivo rotativo en `ingestion/logs/`), reutilizable por todos los productores futuros.
- `ingestion/common/kafka_utils.py`: factory del `Producer` de confluent-kafka con `acks=all` + `enable.idempotence=True` (prioriza no perder/duplicar mensajes), y un callback de entrega que solo loguea.
- `ingestion/weather_producer.py`: loop cada 120s sobre las 5 ciudades PJME, publica en `weather-stream`. Cada ciudad se maneja de forma aislada — si OpenWeatherMap falla para una, las demás siguen (ADR-003: el pipeline no depende de que la API externa esté 100% arriba). Apagado limpio con `SIGINT`/`SIGTERM` (flush antes de salir).
- `ingestion/requirements.txt`: `confluent-kafka`, `python-dotenv`, `requests`.

**Decisión — confluent-kafka vs kafka-python:** se eligió `confluent-kafka` (basado en librdkafka) por ser el cliente con más adopción real en producción; `kafka-python` es más simple pero tiene mantenimiento más discontinuo.

**Verificación (sin Docker/Kafka reales, corrida en el sandbox):** se stubeó `confluent_kafka` y se probó `build_message()` con una respuesta simulada de OpenWeatherMap — mapea bien los 5 campos del esquema y, ante una respuesta con forma inesperada (campos faltantes), devuelve `None` y loguea el error en vez de lanzar una excepción no controlada. `fetch_weather()` no se pudo probar contra la API real (sin salida a internet ni key desde el sandbox) — probalo vos con tu key real.

**Para correrlo (en tu máquina):**
```powershell
cd smart-energy-pipeline
docker compose up -d                          # si no está arriba
pip install -r ingestion/requirements.txt
# completá OPENWEATHER_API_KEY en .env (https://openweathermap.org/api, plan free)
cd ingestion
python weather_producer.py
```
Verificar en Kafka UI (`http://localhost:8085`) que el tópico `weather-stream` recibe mensajes cada 2 minutos.

---

## 2026-07-30 — Fase 1: Terraform local sin costo (LocalStack)

**Contexto:** tercer entregable de Fase 1 — poder correr `terraform apply` de verdad, sin cuenta de AWS ni gasto.

**Se creó / modificó:**
- `docker-compose.yml`: agregado servicio `localstack` (imagen `localstack/localstack:2026.07.1`, solo `SERVICES: s3` porque es lo único que usa `main.tf` por ahora), puerto `4566`.
- `infra/terraform/override.tf.example`: la config manual del provider apuntando a LocalStack (path-style S3, credenciales falsas `test`/`test`).
- `infra/terraform/README.md`: reescrito para recomendar `tflocal` (wrapper oficial de LocalStack, `pipx install terraform-local`) como forma principal — hace automáticamente lo que `override.tf` hace a mano. Se documentan las dos formas.
- `infra/terraform/versions.tf`: provider de AWS actualizado de `~> 5.0` a `~> 6.0` (v6 es GA y es la rama activa de desarrollo; v5 quedó en mantenimiento).

**Límite de esta sesión:** no tengo `terraform` instalado ni salida a `releases.hashicorp.com`/registry en este sandbox (confirmado: `docker`, `terraform` y hasta `pip install` a paquetes externos están bloqueados por el proxy del entorno). No pude correr `tflocal init/plan/apply` yo mismo — el archivo `main.tf` y el bump a v6 quedan sin ejecutar hasta que vos lo corras.

**Para correrlo (en tu máquina):**
```bash
pipx install terraform-local
cd smart-energy-pipeline
docker compose up -d localstack   # o docker compose up -d si no está nada arriba
cd infra/terraform
tflocal init
tflocal plan -var="bronze_bucket_name=smart-energy-bronze-dev"
tflocal apply -var="bronze_bucket_name=smart-energy-bronze-dev"
aws --endpoint-url=http://localhost:4566 s3 ls   # verificar que el bucket existe
```
Si `tflocal init` tira un error de sintaxis en algún recurso, es la señal de que algo cambió entre el provider v5→v6 — avisame y lo corrijo.

---

## 2026-07-30 — Fase 1: incidentes al levantar el stack por primera vez

Dos problemas reales aparecieron al correr `docker compose up -d` en tu máquina (Windows, Docker Desktop) que yo no pude reproducir por no tener Docker en mi entorno:

**1. `airflow-postgres` fallaba el healthcheck en el primer arranque.** Causas: el healthcheck tenía el usuario hardcodeado (`-U airflow`) en vez de leer `${AIRFLOW_DB_USER}`, y no tenía `start_period` — Postgres se reinicia una vez solo durante su inicialización (normal), y si el healthcheck se queda sin reintentos justo en ese momento, Docker lo marca como fallido de arranque. Fix: healthcheck parametrizado + `start_period: 30s` + más reintentos. Confirmado con `docker compose ps -a` en tu máquina: quedó `(healthy)`.

**2. `localstack` moría al toque con "License activation failed" (exit code 55).** Desde la versión `2026.3.0`, LocalStack unificó su imagen community y pro en una sola, y ahora exige `LOCALSTACK_AUTH_TOKEN` (cuenta gratuita en app.localstack.cloud) incluso para arrancar — algo que no sabía al elegir el tag `2026.07.1`. Como el objetivo explícito del proyecto es evitar fricción y cuentas externas, la solución fue pinear a **`localstack/localstack:4.4.0`**, la última versión "community" sin ese requisito, en vez de crear una cuenta. Si en el futuro hace falta una feature nueva de LocalStack, la alternativa documentada en el compose es crear la cuenta gratis y setear `LOCALSTACK_AUTH_TOKEN`.

**Lección para el próximo proyecto:** cuando peguéis una imagen de Docker por versión/fecha reciente sin haberla corrido, verificá igual el comportamiento en el arranque — un cambio de modelo de negocio de la herramienta (community → freemium) no siempre se refleja en el número de versión, y solo se descubre corriendo el contenedor y leyendo sus logs.

---

## 2026-07-30 — Fase 1: primer intento real de `weather_producer.py`

Dos errores al correrlo por primera vez, ninguno era un bug del script:

**1. OpenWeatherMap devolvía `401 Unauthorized` con una key con formato válido.** Es lo esperado: las API keys nuevas de OpenWeatherMap (plan free) tardan hasta ~2 horas en activarse después de creadas. No es un error de código — hay que esperar y reintentar.

**2. `confluent-kafka` no podía conectar: `Connect to ipv6#[::1]:9092 failed`.** En Windows con Docker Desktop, `localhost` a veces resuelve primero a la dirección IPv6 (`::1`), y esa conexión falla aunque el puerto esté bien mapeado y Kafka esté sano — es un problema conocido de librdkafka (el cliente C detrás de `confluent-kafka`) en ese combo específico. Fix: cambiar `KAFKA_BOOTSTRAP_SERVERS` de `localhost:9092` a `127.0.0.1:9092` en `.env` (y en `.env.example`) para forzar IPv4 y saltarse la resolución ambigua.

---

## Plantilla reutilizable para el próximo proyecto

Lo transferible de esta fase, más allá de este proyecto puntual:

1. **Nunca desarrollar con git dentro de una carpeta sincronizada (OneDrive/Dropbox/Drive).** Carpeta de trabajo local, y el remoto (GitHub) es el backup.
2. **Un `docker-compose.yml` con `x-<nombre>-common: &anchor` para servicios repetidos** (como hicimos con Airflow webserver/scheduler) evita duplicar configuración.
3. **`.env.example` versionado, `.env` real gitignorado**, siempre — incluso en proyectos de un solo desarrollador.
4. **Verificar la versión "actual" de cada herramienta antes de fijarla** (ej. Kafka KRaft vs Zookeeper) — el ecosistema de datos cambia rápido y lo que se aprendió hace 1-2 años puede estar obsoleto.
5. **ADR por cada decisión, incluso las que cambian sobre la marcha** — así el proyecto queda defendible sin depender de la memoria.
6. **Terraform contra la nube real cuesta plata incluso "solo probando"; LocalStack + `tflocal` da el mismo `plan`/`apply` real gratis.** El patrón `herramienta.example` versionada + `herramienta` real gitignorada se repite (`.env`/`.env.example`, `override.tf`/`override.tf.example`) — es la forma general de "config con secretos o específica de tu máquina, sin perder el ejemplo documentado".
7. **Cuando el agente (yo) no puede ejecutar algo (Docker, Terraform, red externa), que lo diga explícitamente y deje el comando exacto** en vez de asumir que corrió — mejor un límite claro que una verificación falsa.
