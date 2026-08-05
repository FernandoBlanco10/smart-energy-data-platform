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

## 2026-07-31 — Fase 2: productor de energía (`energy_producer.py`)

**Contexto:** primer entregable de Fase 2 — simular el CSV histórico PJME como stream, mismo contrato que `weather_producer.py` para que Spark trate ambas fuentes igual.

**Se creó:**
- `ingestion/energy_producer.py`: lee `PJME_hourly.csv` fila por fila con `csv.DictReader` (no carga el archivo entero en memoria) y publica 1 fila/seg en `energy-stream`. Filas corruptas (fecha mal formada, MW no numérico, columna faltante) se descartan con log, no tumban el productor — mismo criterio que `weather_producer.py`. Reusa `ingestion/common/` sin cambios.
- `ingestion/data/README.md`: instrucciones para bajar el dataset de Kaggle (no versionado, `.gitignore` ya excluía `*.csv`).
- `.env.example`: agregado `ENERGY_CSV_PATH` (opcional).

**Verificación (sandbox, sin Kafka real):** stub de `confluent_kafka` + CSV de prueba de 3 filas (una corrupta a propósito) — `build_message()` parsea bien fecha y MW, descarta la fila mala sin excepción, y `run()` completo terminó reportando "2 publicadas, 1 descartada", que es el resultado esperado.

**Para correrlo (en tu máquina):** necesitás `PJME_hourly.csv` en `ingestion/data/` (ver el README de esa carpeta — hay que bajarlo de Kaggle, no lo puedo conseguir yo). Con eso:
```bash
cd ingestion
python energy_producer.py
```
Verificar en Kafka UI que `energy-stream` recibe una fila por segundo.

---

## 2026-07-31 — Fase 2: Spark Structured Streaming → Delta Lake (capa Bronze)

**Contexto:** primer job de Spark del proyecto — consume `weather-stream` y `energy-stream` de Kafka y los persiste como tablas Delta en Bronze.

**Se creó:**
- `streaming/common/spark_session.py`: builder de la `SparkSession` con Delta Lake y el conector de Kafka configurados. Versión del conector y sufijo de Scala son variables de entorno, no hardcodeadas — permite bajar a una combinación más probada (Spark 3.5.x/Delta 3.3.x, Scala 2.12) sin tocar código si Spark 4.x/Delta 4.x da problemas en Windows.
- `streaming/common/schemas.py`: los `StructType` que reflejan el JSON de cada productor (`from_json` los necesita explícitos).
- `streaming/common/logging_config.py`: copia deliberada del de `ingestion/` — mismo criterio (logging, no print), pero sin import cruzado entre las dos unidades.
- `streaming/bronze_stream.py`: dos streaming queries en paralelo (una por tópico) dentro de la misma `SparkSession`, cada una escribe Delta en modo `append` con checkpoint propio. Trigger de 10s (no "tan rápido como se pueda") para que los micro-batches sean observables en los logs en vez de spamear.
- `streaming/requirements.txt`: `pyspark==4.0.3`, `delta-spark==4.0.0` — es la combinación documentada oficialmente por delta.io (Scala 2.13). Anoté la alternativa 3.5.x/3.3.x por si hace falta bajar un escalón.

**Decisión — qué significa "crudo" en Bronze:** sin deduplicar, sin filtrar, sin validar rangos (eso es Silver). Pero sí se parsea el JSON a columnas tipadas (`from_json`) más metadata de Kafka (tópico, partición, offset, timestamp) — Bronze como blob de bytes sin parsear sería imposible de inspeccionar sin reprocesar todo a mano.

**Límite de esta sesión:** no tengo Spark ni PySpark instalado en el sandbox (`pip install pyspark` falló por el mismo proxy bloqueado de siempre), así que **no pude ejecutar este job ni una vez** — solo validé sintaxis con `py_compile`. Java 11 sí está disponible acá, así que al menos supe que la versión de Java no es el problema; lo que no puede probarse sin Spark real es si la resolución de versiones (Delta 4.0.0 + Spark 4.0.3 + conector Kafka) funciona sin fricción en Windows. Es la parte de mayor riesgo de todo lo construido hasta ahora — avisame apenas lo corras.

**Confirmado funcionando (2026-08-05, después de los 5 problemas de arriba):** `verify_bronze.py` mostró 30 filas en Bronze/weather y 648 en Bronze/energy, esquema correcto en ambas, datos con sentido. De principio a fin, esta primera pieza de Spark costó: elegir mal la versión (Hadoop 3.4.0 sin winutils disponible), un bug mío pisando `spark.jars.packages`, Kafka caído sin que lo notáramos, y un volumen faltante para que los tópicos sobrevivan reinicios. Todo documentado arriba, todo con fix aplicado.

**Para correrlo (en tu máquina, prerrequisitos nuevos):**
1. Java 11 o 17 instalado (`java -version`).
2. **Windows: winutils.exe** — Spark/Hadoop en Windows lo necesita incluso para escribir a disco local, si no vas a ver un error de `NativeIO$Windows`. Bajalo (versión que matchee Hadoop 3.x, la que trae Spark 4.0.3) a `C:\hadoop\bin\winutils.exe`, seteá `HADOOP_HOME=C:\hadoop` y agregá `C:\hadoop\bin` al PATH.
3. `pip install -r streaming/requirements.txt`
4. Con `weather_producer.py` y `energy_producer.py` corriendo (para que haya datos), en otra terminal: `cd streaming && python bronze_stream.py`

---

## 2026-08-05 — Fase 2: pivote de versiones de Spark/Delta, tres problemas reales en el primer intento

Primer intento de correr `bronze_stream.py` en tu máquina (Windows) — tres fallas distintas:

**1. Productores con conexión inestable a Kafka.** `weather_producer.py` y `energy_producer.py` mostraron varios `FAIL ... Connect to ipv4#127.0.0.1:9092` pero igual lograron publicar (`Ciclo completo: 5/5 ciudades publicadas`). Hipótesis: ruido de reconexión de librdkafka, no necesariamente un bloqueo real — pendiente confirmar con `docker compose ps` que Kafka esté `healthy` de forma sostenida.

**2. `bronze_stream.py` no pudo resolver una dependencia de Delta.** Ivy (el resolutor de dependencias de Spark) falló bajando `io.delta:delta-storage:4.0.0` aunque el artefacto sí existe en Maven Central (lo verifiqué). Es más probable que haya sido un problema transitorio de red o de caché de Ivy corrupta que un error de versión real.

**3. `HADOOP_HOME and hadoop.home.dir are unset`.** El prerrequisito de `winutils.exe` que había avisado, todavía no configurado.

**Decisión — bajar de Spark 4.0.3/Delta 4.0.0 (Scala 2.13) a Spark 3.5.8/Delta 3.3.2 (Scala 2.12):** investigando el problema 3, encontré que el repo comunitario más usado para conseguir `winutils.exe` en Windows (`cdarlint/winutils`) **no tiene binarios para Hadoop 3.4.0**, que es lo que Spark 4.0.x empaqueta — llega hasta Hadoop 3.3.6. Sin un `winutils.exe` que matchee, Spark no arranca en Windows, punto. Spark 3.5.x empaqueta Hadoop 3.3.4, y para esa línea sí hay binarios (3.3.5/3.3.6, suficientemente cerca para funcionar). Alternativas descartadas: compilar `winutils.exe` para 3.4.0 a mano (viable pero mucho esfuerzo para lo que aporta) y usar WSL2 en vez de Windows nativo para correr Spark (más "correcto" pero es otro salto de infraestructura en medio de la fase, se pospone). `streaming/requirements.txt` y `streaming/common/spark_session.py` ya actualizados.

**Todavía sin confirmar (para tu próximo intento):** limpiar la caché de Ivy (`Remove-Item -Recurse -Force "$env:USERPROFILE\.ivy2.5.2"`) para descartar el problema 2, y completar el setup de `winutils.exe` para Hadoop 3.3.6.

**Actualización — Kafka no estaba corriendo.** `docker compose ps` mostró que solo `localstack` seguía arriba; `kafka` y el resto se habían caído (probablemente un reinicio de Docker Desktop). `Test-NetConnection` confirmó el puerto 9092 ni siquiera respondía a TCP crudo — no era un problema de Spark ni de Kafka en sí. `docker compose up -d` lo resolvió.

**Actualización — el contenedor de Kafka no tenía volumen persistente.** Al volver a levantar Kafka "limpio", `bronze_stream.py` falló con `UnknownTopicOrPartitionException: This server does not host this topic-partition`. Causa: el `AdminClient` que usa Spark para listar particiones de un tópico **no lo crea** si no existe (a diferencia de un productor, que sí dispara `KAFKA_AUTO_CREATE_TOPICS_ENABLE`). Con un broker recién reiniciado y sin productores corriendo todavía, `weather-stream`/`energy-stream` genuinamente no existían. Se agregó `kafka-data:/var/lib/kafka/data` como volumen al servicio `kafka` en `docker-compose.yml` para que los tópicos sobrevivan un reinicio del contenedor — hasta ahora era el único servicio con estado que no tenía volumen (Postgres y LocalStack sí).

**Orden operativo correcto de acá en adelante:** productores arriba y con al menos un ciclo publicado (confirmable en Kafka UI) **antes** de arrancar cualquier job de Spark que lea de esos tópicos.

**Actualización — winutils y Delta funcionaron a la primera con la nueva versión.** El segundo intento resolvió Delta sin problema y pasó de largo el error de Hadoop. Apareció un error nuevo, distinto: `Failed to find data source: kafka`. Causa: bug mío en `spark_session.py` — `configure_spark_with_delta_pip()` arma su propio `spark.jars.packages` para los jars de Delta, y como yo también seteaba `spark.jars.packages` a mano en el builder para agregar el conector de Kafka, uno pisaba al otro (ganaba Delta, Kafka desaparecía en silencio del log de Ivy). Fix: pasar el paquete de Kafka vía el parámetro `extra_packages=[...]` que la función de Delta expone justamente para este caso, en vez de setear `spark.jars.packages` por separado.

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
