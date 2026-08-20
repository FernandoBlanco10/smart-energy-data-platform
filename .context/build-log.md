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

## 2026-08-05 — Fase 2: Spark Structured Streaming → Delta Lake (capa Silver)

**Contexto:** segundo job de Spark — lee Bronze (no Kafka) y escribe la versión limpia.

**Se creó:**
- `streaming/common/quality_rules.py`: rangos válidos (`WEATHER_RANGES`, `ENERGY_MW_RANGE`) centralizados — los va a usar también el test de calidad de la task 8, para no tener el mismo número mágico en dos archivos.
- `streaming/silver_stream.py`: lee `bronze/weather` y `bronze/energy` como streams de Delta (no de Kafka — desacopla Silver de la retención del tópico), tipa el timestamp con `to_timestamp`, deduplica con `dropDuplicatesWithinWatermark` (Spark 3.5+, más simple que el patrón viejo de watermark+dropDuplicates), y filtra a los rangos de `quality_rules.py`.
- `streaming/verify_silver.py`: como `verify_bronze.py`, más un cálculo de cuántas filas se descartaron en la limpieza (Bronze count - Silver count).

**Decisión de diseño — Silver lee de Bronze, no de Kafka de nuevo:** si hay que reprocesar Silver por un bug de limpieza, Bronze ya tiene todo el historial — no depende de la retención de Kafka. Es el patrón estándar de arquitectura medallion.

**Decisión de diseño — watermark de 10 minutos es tiempo de evento, no de reloj:** para `energy`, cada fila sucesiva salta 1 hora entera en su propio timestamp (son datos históricos reproducidos rápido) — una ventana chica alcanza para atrapar reintentos reales sin acumular estado de más.

**Límite de esta sesión:** mismo que Bronze — sin Spark en el sandbox, solo validado por `py_compile`. A diferencia de Bronze, esta vez no tuve que adivinar versiones (ya está resuelto desde la fase anterior), así que el riesgo de fricción nueva debería ser menor — pero el patrón de `dropDuplicatesWithinWatermark` es nuevo en este proyecto, sin probar en la práctica todavía.

**Actualización — bug real encontrado con datos, no con la ejecución.** Primera corrida: `verify_silver.py` mostró weather en 0% descartado (esperable) pero energy en 12.9% descartado (1043 → 908 filas) — sospechoso, porque el dataset debería tener casi cero duplicados reales. Se investigó el CSV directamente: `PJME_hourly.csv` **no está ordenado cronológicamente de punta a punta** (6041 "retrocesos" verificados con un script — la fila siguiente tiene fecha anterior a la actual), aunque solo tiene 4 timestamps exactamente duplicados (por el cambio de horario de otoño en EE. UU., un patrón conocido de este dataset).

Causa real: el watermark de 10 minutos que usaba `clean_energy()` no solo limpia estado viejo — en Structured Streaming, cualquier fila que llegue con un `event_time` más viejo que `(máximo visto - watermark)` se descarta directamente, tratándola como "dato tarde". Con un CSV que salta hacia atrás en el tiempo constantemente, eso tiraba filas válidas, no duplicados.

**Fix:** `clean_energy()` ya no usa watermark — usa `dropDuplicates()` simple. Es seguro acá porque `energy-stream` es un stream **acotado** (el CSV se termina), a diferencia de `weather-stream` que es infinito mientras el productor corra. Con ~145k filas como techo, el estado que Spark tiene que recordar para deduplicar es trivial en memoria, así que no purgarlo nunca (la desventaja de no tener watermark) no es un problema real acá. `clean_weather()` se queda con el watermark — ahí sí es un stream infinito y si no se libera estado viejo, crece para siempre.

**Para correrlo:** con `bronze_stream.py` corriendo y habiendo escrito al menos un micro-batch:
```bash
cd streaming
python silver_stream.py       # otra terminal
python verify_silver.py       # para confirmar, en una más
```

---

## 2026-08-08 — Fase 2: particionado físico en Bronze y Silver

**Contexto:** hasta acá `bronze_stream.py` y `silver_stream.py` escribían cada tabla Delta como una sola carpeta plana — sin `partitionBy()`. Funcionaba, pero no había nada que "observar" en el filesystem, y en un dataset real sin partición Spark tiene que leer todos los archivos aunque la query pida un solo día o una sola ciudad.

**Se agregó:**
- `bronze_stream.py`: columna `ingest_date` (`to_date(ingested_at)`) + `.partitionBy("ingest_date")`. Partición por tiempo de **llegada**, no de evento — a propósito, porque ya sabemos (bug de Silver arriba) que el tiempo de evento puede venir desordenado. `ingest_date` es siempre monótona (es "ahora"), así que particionar por ella nunca se rompe sin importar qué tan sucio venga el dato de origen. Es el patrón estándar de una zona de aterrizaje ("landing zone").
- `silver_stream.py`: `clean_weather()` particiona por `city` (5 valores fijos, PJME/OWM). `clean_energy()` agrega la columna `event_year` (`year(event_time)`) y particiona por ella — PJME cubre 2002-2018, así que año da ~16 carpetas de tamaño razonable (particionar por día con datos horarios hubiera dado miles de carpetas chiquitas, el "small file problem"). Estas sí son particiones de **negocio**: pensadas para cómo alguien va a consultar Silver después ("dame Baltimore", "dame 2015"), no para cuándo llegó el dato.
- `ingestion/energy_producer.py`: `SECONDS_PER_ROW` ahora lee `ENERGY_SECONDS_PER_ROW` del entorno (default 1.0, sin cambio de comportamiento). A 1 fila/segundo, publicar el CSV completo (145,367 filas) tarda ~40 horas — para observar en minutos cómo se van formando carpetas `event_year=.../` en Silver hace falta acelerarlo puntualmente.

**Tres conceptos de "partición" distintos en este pipeline, para no confundirlos:**
1. **Partición de Kafka** (paralelismo de mensajería): cuántas particiones tiene un tópico, cómo Spark las consume. Se observa en Kafka UI (`localhost:8085`) o `kafka-topics.sh --describe`.
2. **Partición de Spark en memoria** (`spark.sql.shuffle.partitions`, hoy en 4): paralelismo de cómputo dentro de un job. Se observa en la Spark UI (`localhost:4040`, activa mientras `bronze_stream.py`/`silver_stream.py` corren) — pestaña Stages, columna Tasks.
3. **Partición de Delta Lake en disco** (`partitionBy`, lo agregado hoy): cómo se organizan los archivos Parquet en subcarpetas Hive-style (`event_year=2015/part-...parquet`). Se observa directamente en el filesystem.

**Para correrlo desde cero:**
```bash
# 1. Detener todo lo que esté corriendo (Ctrl+C en cada terminal)
docker compose down -v          # borra volúmenes: kafka-data, postgres-db-volume, localstack-data
rm -rf streaming/lakehouse       # borra bronze/, silver/ y todos los checkpoints
docker compose up -d
docker compose ps                # esperar a que todo esté healthy

# 2. Productores (una terminal cada uno)
cd ingestion
python weather_producer.py
ENERGY_SECONDS_PER_ROW=0.02 python energy_producer.py   # acelerado solo para este ejercicio

# 3. Bronze (otra terminal)
cd streaming
python bronze_stream.py

# 4. Silver (otra terminal)
cd streaming
python silver_stream.py

# 5. Verificar (otra terminal, cuando quieras un snapshot)
cd streaming
python verify_silver.py
```

**Dónde mirar mientras corre:** Kafka UI en `localhost:8085` (particiones y mensajes por tópico); Spark UI en `localhost:4040` (tasks por stage = shuffle partitions); `ls -R streaming/lakehouse/bronze/weather` y `ls -R streaming/lakehouse/silver/energy` (carpetas `ingest_date=`/`city=`/`event_year=` apareciendo a medida que llegan datos nuevos).

---

## 2026-08-08 — Fase 2: cierre de Silver — el 26.3% descartado en energy también era real, y correcto

**Contexto:** después del fix del watermark, `verify_silver.py` mostró weather en 0% descartado (esperado) pero energy subió a 26.3% (peor que el 12.9% original) — parecía que el fix había empeorado las cosas.

**Diagnóstico:** sin acceso a Spark/Parquet en mi sandbox (PyPI está bloqueado por la política de red del workspace, no pude instalar `pyarrow`), se armó `streaming/debug_energy_duplicates.py` para que el usuario lo corriera con su propio PySpark. Agrupa Bronze/energy por `timestamp` y muestra los `kafka_offset` de cada grupo repetido.

**Resultado:** 949 timestamps con exactamente 2 apariciones cada uno, y en todos los casos la diferencia entre los dos offsets ronda los ~949 (ej. `[0, 949]`, `[2, 951]`, `[7, 956]`). Ese patrón consistente es la firma de un **reinicio único** de `energy_producer.py` a mitad de la sesión (por `docker compose down -v` o por recuperarse del `UnknownTopicOrPartitionException`) — como el productor no persiste ningún cursor de progreso, cada reinicio vuelve a leer el CSV desde la fila 1, republicando las mismas filas de negocio con offsets nuevos.

**Conclusión:** `dropDuplicates(["grid_region", "event_time"])` está correcto. El 26.3% no era pérdida de datos — era la capa de Silver descartando duplicados genuinos causados por reinicios del productor durante el debugging de esta sesión, exactamente el escenario para el que existe esta capa. Task 7 (Silver) queda cerrada con esta validación.

**Nota para producción real (no implementado, fuera de alcance de este proyecto):** un productor de nivel profesional normalmente persistiría su posición (ej. guardar el número de fila ya publicada, o usar Kafka idempotent producer + claves) para no reprocesar desde cero en cada reinicio. Acá se dejó así a propósito — sirvió justamente para probar que Silver es robusto ante este tipo de duplicado real.

---

## 2026-08-09 — Fase 2: task 8, tests de calidad de datos en Silver

**Se creó:** `streaming/test_silver_quality.py` — script batch independiente, no forma parte del pipeline de streaming. Reutiliza `common/quality_rules.py` (mismas reglas que usa `silver_stream.py` al limpiar) pero las vuelve a chequear por separado sobre Silver ya escrito: sin nulls en columnas clave, valores dentro de rango físico, unicidad de la clave de negocio. Termina con código de salida 1 si algo falla — pensado para poder engancharse a un CI real en el futuro.

**Resultado de la primera corrida real, con el stack completo corriendo (productores + bronze + silver activos al mismo tiempo):** 14/14 checks OK, incluida la unicidad — confirmación adicional de que el dedup de Silver sigue sosteniendo la invariante incluso con datos llegando en simultáneo.

Fase 2 queda funcionalmente completa: productores → Kafka → Bronze (particionado por `ingest_date`) → Silver (particionado por `city`/`event_year`, deduplicado, validado) → test de calidad independiente, todo verificado con datos reales, no solo con `py_compile`.

---

## 2026-08-09 — Fase 3: scaffold de dbt sobre Silver (dbt-spark, modo session)

**Contexto:** ADR-006 ya había decidido dbt para transformación, pero no el motor. Se decidió (ADR-009, con el usuario eligiendo entre dos opciones presentadas) `dbt-spark` en modo `session` sobre `dbt-duckdb` — reutiliza el Spark+Delta ya armado en Fase 2, cero herramientas nuevas.

**Se creó:**
- `transformation/requirements.txt` — `dbt-core==1.12.0`, `dbt-spark[session]==1.11.0`. Verificadas como último release estable en PyPI a la fecha (ambas publicadas el mismo día, 2026-07-16 — probable que sea la pareja pensada para ir junta).
- `transformation/dbt_project/dbt_project.yml` — con `on-run-start` que registra `silver_weather`/`silver_energy` como tablas externas Delta (`CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION ...`) antes de cada corrida.
- `transformation/dbt_project/models/staging/sources.yml` — define esas dos tablas como `source` de dbt.
- `transformation/spark-defaults.conf` + `profiles.yml.example` — ver más abajo, el punto de fricción real de esta fase.
- `.gitignore` actualizado: `transformation/profiles.yml` (config local, como `.env`) y artefactos de dbt (`target/`, `dbt_packages/`, `logs/`).

**Problema real anticipado (todavía sin confirmar con ejecución):** dbt no lee Delta directamente — necesita que las tablas existan en el catálogo (Hive metastore) de la SparkSession, algo que Databricks/Unity Catalog resuelve solo pero que acá hay que armar a mano con `on-run-start`. Peor: el modo `method: session` de `dbt-spark` arma su propia SparkSession internamente, sin exponer en `profiles.yml` un lugar para pasarle `spark.jars.packages` (necesario para que Spark sepa qué es Delta). Como una SparkSession es un singleton por JVM, no se puede "inyectar" Delta después de que dbt ya la creó.

**Solución:** `spark-defaults.conf` con `spark.jars.packages`/`spark.sql.extensions`/`spark.sql.catalog.spark_catalog`, y la variable de entorno `SPARK_CONF_DIR` apuntando a esa carpeta. PySpark lee ese archivo automáticamente al arrancar *cualquier* SparkSession, sin importar qué código la crea — es el mismo mecanismo que usa un cluster Spark real en producción para que toda la config de jars/extensiones sea uniforme entre apps, en vez de que cada script la hardcodee (como sí hace `streaming/common/spark_session.py`, a propósito, porque ese código sí controla el builder directamente).

**Límite de esta sesión:** todo lo anterior es diseño razonado, no ejecutado — sin dbt/Spark en mi sandbox, la primera prueba real es que el usuario corra `dbt debug` desde `transformation/dbt_project/` con las variables de entorno del README seteadas. Es zona de fricción esperable (Windows + JVM + classpath), mismo tipo de problema que winutils en Fase 2.

**Actualización — funcionó a la primera.** `dbt debug` resolvió el jar de Delta vía Ivy (mismo mecanismo que Bronze/Silver, disparado esta vez por `spark-defaults.conf` + `SPARK_CONF_DIR`) y pasó el connection test. El truco de configurar Delta por fuera de `profiles.yml` para el modo `session` quedó validado con ejecución real, no solo en teoría. Task 15 cerrada.

---

## 2026-08-11 — Fase 3: modelo intermedio — el join clima+consumo no es por fecha

**Problema real de diseño, detectado antes de escribir código:** el roadmap pedía "unir clima y consumo por ventana temporal", asumiendo que ambas fuentes viven en la misma línea de tiempo. No es así: `weather-stream` es en vivo (event_time = fecha de hoy), `energy-stream` es el histórico PJME 2002-2018 reproducido como stream (event_time = fecha real del CSV). Un join por timestamp exacto siempre da vacío — nunca hay una fila de weather de 2026 que coincida con una de energy de 2007.

**Decisión (con el usuario eligiendo entre dos opciones):** unir por `HOUR(event_time)` (0-23) en vez de por fecha exacta — comparar el patrón típico de consumo por hora del día contra el patrón típico de temperatura por hora del día, sin importar el año. Se descartó conseguir un dataset de clima histórico real 2002-2018 para poder unir por fecha exacta: es un desvío grande de alcance (nueva fuente, nueva ingesta) fuera de lo planeado para esta fase.

**Se creó:**
- `int_energy_hourly.sql` — `LAG`/`LEAD` hora a hora sobre consumo, promedio móvil de 24 horas (`ROWS BETWEEN 23 PRECEDING AND CURRENT ROW`), más `hour_of_day`.
- `int_weather_readings.sql` — mismo patrón pero `PARTITION BY city` (weather no llega en grilla horaria, llega cada ~2 min), promedio móvil de 5 lecturas en vez de 24 horas.
- `int_hourly_climate_demand_pattern.sql` — el join real: agrega cada fuente por `hour_of_day` y las une con `FULL OUTER JOIN` (a propósito, no `INNER JOIN`, para que sea visible si weather todavía no cubre las 24 horas del día — los conteos `n_energy_readings`/`n_weather_readings` lo dejan explícito en vez de esconder filas).

**Límite de esta sesión:** sin ejecutar todavía — sintaxis de Jinja/YAML validada, pero la prueba real es `dbt build` con el productor de weather habiendo corrido el tiempo suficiente para tener más de una o dos horas del día representadas.

---

## 2026-08-11 — Fase 3: capa Gold (fct_/dim_) + docs

**Se creó:**
- `seeds/dim_city.csv` — dimensión estática de las 5 ciudades (con `state`, dato que nunca viaja por Kafka/Silver). Primer uso de un dbt **seed** en el proyecto: data de catálogo que no se deriva de ninguna fuente, se declara a mano como CSV versionado.
- `models/gold/fct_energy_consumption_hourly.sql` — graduación de `int_energy_hourly` a tabla física, con `hour_over_hour_change_mw` como medida derivada nueva.
- `models/gold/fct_weather_readings.sql` — join real con `dim_city` (enriquece con `state`/`region`, algo que Silver no tiene).
- `models/gold/fct_hourly_climate_demand_pattern.sql` — graduación directa de `int_hourly_climate_demand_pattern`, sin lógica nueva.
- `gold.yml`/`seeds.yml` — docs + tests básicos (`not_null`, `unique`, `relationships` de `fct_weather_readings.city` hacia `dim_city`). Tests más completos quedan para la task 19 a propósito.

**Decisión de diseño — por qué solo una dimensión (`dim_city`) y no una por cada atributo:** `grid_region` (siempre "PJME") y `hour_of_day` (0-23) se quedan como columnas directas en sus tablas de hechos en vez de tener su propia `dim_`, porque no tienen ningún atributo descriptivo propio más allá del valor en sí — son "dimensiones degeneradas", un concepto real de modelado dimensional (Kimball). `city` sí ameritó una dimensión de verdad porque `dim_city` le agrega algo que no estaba en los datos de origen (`state`).

**Límite de esta sesión:** sin ejecutar — la verificación real de task 17 y 18 va a quedar confirmada juntas con el próximo `dbt build` (Gold depende de intermediate, así que corre todo el DAG de una vez).

**Actualización — corrió, 30/30 PASS, pero con dos defectos reales encontrados en el log:**

1. **Gold no se estaba escribiendo como Delta.** El log mostró `"A Hive serde table will be created as there is no table provider specified"` para `dim_city` y cada `fct_*` — sin `file_format` explícito, dbt-spark usa el default de Spark (tabla Hive genérica), no Delta. Rompía la consistencia "todo el lakehouse es Delta" sostenida desde Bronze/Silver.
2. **Ubicación equivocada.** Las tablas quedaron en `transformation/dbt_project/spark-warehouse/` (default de Spark para tablas administradas), no en `streaming/lakehouse/gold/` junto a Bronze/Silver.

**Fix:** `+file_format: delta` y `+location_root: "{{ env_var('LAKEHOUSE_BASE_PATH') }}/gold"` en el bloque `gold:` de `dbt_project.yml` (y en `seeds:` para `dim_city`). También se corrigió un `[WARNING][DeprecationsSummary]` del log: la sintaxis vieja del test `relationships` (sin el wrapper `arguments:`) — dbt Core 2.0 ya está en beta y va a eliminar la sintaxis vieja, mejor corregirlo ahora que se vio que existía.

**Todavía sin confirmar con ejecución:** la combinación `file_format`+`location_root` juntos no se probó todavía. Recomendado `dbt build --full-refresh` para forzar la reconstrucción completa en el lugar/formato nuevo, en vez de depender de que dbt detecte solo el cambio.

**Actualización — `location_root` no es compatible con `table` en dbt-spark/Delta.** Con el catálogo y el filesystem ya limpios, siguió fallando: `org.apache.spark.sql.AnalysisException: Table ... does not support truncate in batch mode`, disparado desde `AtomicReplaceTableAsSelectExec`. Causa real (confirmada con el stack trace completo, no una suposición): la materialización `table` de dbt-spark reconstruye cada corrida con `CREATE OR REPLACE TABLE ... AS SELECT`, que en Spark pasa por el path de DataSourceV2 y requiere que la tabla soporte truncar atómicamente. Delta, cuando la tabla tiene una `LOCATION` explícita (lo que hace `location_root`), no expone esa capacidad por esta vía — es una limitación real de la combinación Spark 3.5 + Delta 3.3.2 + DataSourceV2, no un problema de nuestra config.

**Fix:** se sacó `location_root` de `gold:`/`seeds:` en `dbt_project.yml`, se dejó `file_format: delta`. Gold queda en Delta real (lo importante) pero en la ubicación default de Spark (`transformation/dbt_project/spark-warehouse/`) en vez de junto a Bronze/Silver — trade-off consciente, documentado acá en vez de perseguir la ubicación "prolija" a costa de más fragilidad. `dim_city` (el seed) nunca tuvo este error — el `INSERT` de seeds no pasa por el mismo camino de "replace atómico", así que en teoría podría haber mantenido `location_root` solo ahí, pero se sacó de los dos por consistencia y simplicidad (una sola regla, no dos casos distintos que recordar).

**Actualización — el error persistió idéntico incluso sin `location_root`, con tabla nueva.** Mismo `does not support truncate in batch mode`, mismo `AtomicReplaceTableAsSelectExec`, esta vez con un stack trace completo que confirmó la causa real: no es la ubicación, es que la materialización `table` de dbt-spark siempre reconstruye con `CREATE OR REPLACE TABLE ... AS SELECT`, y Delta 3.3.2/Spark 3.5.8 no soporta esa operación de reemplazo atómico por el path DataSourceV2 que eso dispara — límite real de esta combinación de versiones, reproducido igual en 4 intentos distintos. Decisión final (**ADR-010**): Gold en Parquet nativo, no Delta. Bronze/Silver se quedan en Delta (ahí sí importa: streaming, escrituras incrementales). Se acepta perder ACID/time-travel en Gold porque se reconstruye entera cada corrida, sin concurrencia.

---

## 2026-08-13 — Fase 3: task 19, tests completos sobre Gold

**Se agregó:**
- `packages.yml` — primer paquete externo de dbt (`dbt-labs/dbt_utils`, versión 1.4.1 verificada como última release en GitHub). Se usa solo por `accepted_range`, que dbt-core no trae de fábrica.
- `gold.yml` ampliado: `accepted_values` en `grid_region` (siempre "PJME") y `condition` (las categorías reales que devuelve OpenWeatherMap); `dbt_utils.accepted_range` en `consumption_mw`, `temperature_celsius`, `humidity_percentage`, `wind_speed_m_s` — reusando los mismos números de `streaming/common/quality_rules.py`, no reinventados.

**Decisión de diseño:** estos tests repiten, en Gold, las mismas reglas que ya aplica `silver_stream.py` al limpiar y que ya vuelve a chequear `test_silver_quality.py` en Silver. Es intencional, mismo criterio que motivó la task 8 de Fase 2: cada capa se valida a sí misma de forma independiente, sin asumir que la capa de abajo garantiza para siempre que todo sigue bien.

**Actualización — corrió a la primera, 37/37 PASS.** `dbt deps` instaló dbt_utils 1.4.1 sin problema, y los 7 tests nuevos (accepted_values x2, accepted_range x4, más los que ya había) pasaron todos. Fase 3 tiene ahora Bronze→Silver→staging→intermediate→Gold con calidad de datos verificada en cada capa de negocio (Silver con `test_silver_quality.py`, Gold con estos tests de dbt). Task 19 cerrada.

---

## 2026-08-13 — Fase 3: task 20, prototipo agente NL→SQL

**Se agregó (`agents/nl_query_agent/`):**
- `build_gold_catalog.py` — construye/refresca `transformation/gold.duckdb`: vistas DuckDB sobre las tablas Gold que dbt ya escribió en `spark-warehouse/` (`read_parquet()` para los 3 `fct_*`, extensión `delta`/`delta_scan()` para `dim_city`). No corre dbt ni copia datos, solo declara vistas.
- `agent.py` — loop de tool-use con la Claude API (`claude-sonnet-5` por default, configurable): recibe una pregunta, el modelo genera SQL vía la tool `run_sql_query`, se valida y ejecuta contra `gold.duckdb` abierta en modo `read_only=True`, y el modelo devuelve una explicación en texto del resultado. Tope duro de `MAX_TOOL_ITERATIONS=5`.
- `logging_config.py` — copia deliberada del patrón ya usado en `ingestion/common/` y `streaming/common/` (unidades independientes, ver esos módulos).
- `requirements.txt`, `README.md`.
- `.env`/`.env.example`: `ANTHROPIC_API_KEY`, `NL_QUERY_AGENT_MODEL`, `SPARK_WAREHOUSE_PATH`, `GOLD_DUCKDB_PATH`.
- `.gitignore`: `transformation/gold.duckdb` (artefacto generado, se reconstruye con `build_gold_catalog.py`).

**Decisión de diseño — DuckDB en vez de reutilizar dbt-spark/Spark para este agente (ADR-011):** Gold ya es Parquet nativo (ADR-010) más `dim_city` en Delta. Levantar una SparkSession solo para leer eso es sobre-ingeniería — varios segundos de arranque de JVM por consulta, sin ningún beneficio, ya que el agente nunca escribe. DuckDB lee Parquet nativo y Delta (vía su extensión oficial) sin JVM, arranque casi instantáneo. Esto no contradice ADR-009 (que descartó dbt-duckdb para la capa de *transformación*, un problema de escritura): acá el problema es de lectura pura, terreno donde DuckDB es fuerte.

**Cómo se resolvió el guardrail central de `docs/agent-layer-spec.md`** ("el permiso SELECT-only tiene que ser una restricción real de credenciales, no una instrucción de prompt"): `agent.py` abre `gold.duckdb` con `duckdb.connect(path, read_only=True)`. Esa es una restricción real a nivel de motor de datos — cualquier sentencia de escritura que se colara hasta ahí la rechaza DuckDB, no el prompt. Como defensa adicional (no la única), `validate_select_only()` rechaza en código cualquier SQL que no empiece con `SELECT`/`WITH` o que contenga una palabra clave de escritura, antes de ejecutar nada.

**Pendiente de confirmar por el usuario:** correr `pip install -r agents/nl_query_agent/requirements.txt`, completar `ANTHROPIC_API_KEY` en `.env`, correr `python build_gold_catalog.py` y luego `python agent.py "<pregunta>"` con Gold ya construida (`dbt build` corrido al menos una vez). No pude instalar/ejecutar esto en mi sandbox (PyPI bloqueado, sin acceso a los archivos reales de `spark-warehouse/` del usuario) — código escrito y documentado, pero no verificado con una corrida real todavía.

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
