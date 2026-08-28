# Bitácora de aprendizaje

Este archivo lo llenás vos al cerrar cada fase. No es documentación del sistema — es documentación de **tu propio entendimiento**, y es lo que te va a servir para defender el proyecto con seguridad en vez de recitar el `README.md`.

Por cada fase, respondé estas cuatro preguntas en 2-4 líneas cada una. No copies definiciones — escribilo con tus palabras, como se lo explicarías a alguien que no sabe nada del proyecto.

---

## Fase 1 — Infraestructura y entorno local

- **¿Qué entendí que antes no entendía?**
  Entendí la importancia de levantar un proyecto desde cero pensándolo y estructurándolo bien desde antes, con las alternativas mejor acopladas a las necesidades. Antes me costaba aterrizar el por qué de la nube y de las herramientas que había visto por separado; con esta fase entiendo cómo se conectan entre sí y cómo dividir la arquitectura por funciones. Comprendí para qué sirve Kafka y cómo se va a comportar en mi proyecto, parte importante del streaming y procesamiento de datos. Descubrí LocalStack como alternativa para practicar sin depender de pagar servicios de nube — es algo en lo que debo profundizar más para entender qué pasa adentro de la máquina cuando se levantan estas herramientas. Aprendí la importancia y el uso de Terraform, una herramienta "mágica" para el versionado de servicios y arquitecturas. Aterricé también Docker, y espero seguir aprendiendo sobre la marcha hasta entender Docker y Kubernetes a fondo. Todavía necesito repasar los pasos y comandos para levantar todo desde cero, pero ahora entiendo mucho mejor la arquitectura y cómo se arma dependiendo de los requerimientos del proyecto.

- **¿Qué error cometí y cómo lo resolví?**
  Trabajé sobre una carpeta sincronizada con OneDrive, lo cual rompió git de forma difícil de diagnosticar. Ahora sé que los proyectos van en carpetas que no se sincronizan, subiendo el avance al repositorio con commits claros que especifiquen la tarea realizada. También aprendí a tener cuidado al instalar LocalStack: hay que usar la versión gratuita y no la de paga, porque esta última pide credenciales y eso frena todo el flujo si no las tenés.

- **¿Qué decisión de arquitectura cuestioné o cambié, y por qué?**
  Usar KRaft en vez de Zookeeper — porque ya viene con Kafka, no se necesita un servicio externo, y ahora Kafka administra su propio estado.

- **Si me preguntan "explicame esta fase en 30 segundos", ¿qué digo?**
  Esta fase sirvió para dejar lista la infraestructura necesaria, agnóstica a la nube y funcional para las siguientes etapas del proyecto: el orquestador (Airflow), el sistema de mensajería distribuido (Kafka), el data lake donde se va a almacenar la información (S3 vía LocalStack) y la red de cómo se van a comunicar todas las piezas entre sí.

---

## Fase 2 — Streaming, Lakehouse e ingesta batch

- **¿Qué entendí que antes no entendía?**
  Ahora entendí cómo opera Kafka, cuál es el trabajo de Kafka y el de Spark. No había aterrizado bien los conceptos de offset, partición, cluster, broker pero ahora ya entiendo cómo se presentan en el proyecto. Igual cómo Spark trata los datos que llegan tarde, cómo se usan las windows y las watermarks para recuperar datos tardíos.

- **¿Qué error cometí y cómo lo resolví?**
  Cometí error al no analizar tan profundamente por qué el porcentaje de datos filtrados incrementó. Igual fue error al bajar Docker y volverlo a subir, debo tener bien mapeados esos cambios y cómo afectan a un código que lee un dataset desde el principio.

- **¿Qué decisión de arquitectura cuestioné o cambié, y por qué?**
  El acomodo de carpetas en Bronze y Silver: agregué particionado físico en las dos capas, pero con un criterio distinto en cada una a propósito. Bronze se particiona por fecha de ingestión (`ingest_date`) — cuándo llegó el dato a mi sistema, no cuándo ocurrió el evento — porque ya sé que el tiempo de evento puede venir desordenado, y así nunca hay que tocar particiones viejas. Silver en cambio particiona por columnas de negocio (`city` en weather, `event_year` en energy), pensado para cómo se va a consultar después, no para cuándo llegó.

- **Si me preguntan "explicame esta fase en 30 segundos", ¿qué digo?**
  Esta fase es el corazón del flujo del pipeline, tener los datos crudos de alguna manera como se esperan para poder procesarlos y mantenerlos consistentes en la capa Silver. No solo es de procesarlos una vez, sino monitorear, testear y confirmar que ambas capas trabajan bien para continuar con el pipeline. Se debe ser muy cuidadoso con dejar pasar datos corruptos por temas de optimización, eficiencia y limpieza en los datos que ya se ocuparán para la analítica de negocio. Mantener el control en estas dos etapas es fundamental porque es la parte sucia del trabajo de un pipeline; teniendo estas dos capas resueltas, lo demás te deja más tranquilo para trabajar con datos limpios.

---

## Fase 3 — Transformación con dbt y agente de consulta

- **¿Qué entendí que antes no entendía?**
  Entendí cómo se toman decisiones importantes para la arquitectura de datos. Comprendí cómo es que para proyectos con más capacidad de datos se consideran cosas importantes como el linaje de los datos, el time travel, ACID y también pensar en qué pasará en el futuro. Por ejemplo, al ser este un proyecto relativamente pequeño, la capa Gold se puede eliminar y volver a crear con datos nuevos, pero si fueran tablas con millones de datos se tendría que pensar en una arquitectura que compruebe qué datos son nuevos, cuáles cambiaron y cuáles se eliminaron para realizar lo correspondiente sin levantar una tabla de millones de registros que pueda tardar mucho tiempo en hacerse y gastar muchos recursos.

- **¿Qué error cometí y cómo lo resolví?**
  Los errores que tuve fueron más de versiones en los paquetes de las tecnologías que se utilizaron, se resolvió haciendo investigación para actualizarse sobre las capacidades que se tienen actualmente estos paquetes para que no sea un tema el correr el flujo y observar demasiados mensajes de error.

- **¿Qué decisión de arquitectura cuestioné o cambié, y por qué?**
  La de pasar la capa Gold a Parquet, ya que había problemas con dbt al conectarse con la API de Spark que hacía la operación de truncar la tabla y volverla a crear — Delta no era compatible con esta acción — y se optó mejor por pasar Gold a Parquet ya que es una tabla que se regenera y la información de versionamiento ya se tiene en la capa Silver, por lo que no afecta.

- **Si me preguntan "explicame esta fase en 30 segundos", ¿qué digo?**
  Esta fase consiste en no solo generar stored procedures de SQL, sino ir más allá: generar algo que nos brinde linaje de datos, versionado de tablas, tests, documentación y orquestación al regenerar la capa. Esto nos ayuda a la mantenibilidad de lo que se ejecuta, de dónde viene la información y hacia dónde va — esta metadata nos da la flexibilidad de implementar NL→SQL que conozca cómo están las relaciones.

---

## Fase 4 — ML, dashboard, agente de monitoreo y CI/CD

- **¿Qué entendí que antes no entendía?**
- **¿Qué error cometí y cómo lo resolví?**
- **¿Qué decisión de arquitectura cuestioné o cambié, y por qué?**
- **Si me preguntan "explicame esta fase en 30 segundos", ¿qué digo?**

---

## Fase 5 — Agentes avanzados (opcional)

- **¿Qué entendí que antes no entendía?**
- **¿Qué error cometí y cómo lo resolví?**
- **¿Qué decisión de arquitectura cuestioné o cambié, y por qué?**
- **Si me preguntan "explicame esta fase en 30 segundos", ¿qué digo?**
