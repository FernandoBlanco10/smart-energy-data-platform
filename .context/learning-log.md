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
- **¿Qué error cometí y cómo lo resolví?**
- **¿Qué decisión de arquitectura cuestioné o cambié, y por qué?**
- **Si me preguntan "explicame esta fase en 30 segundos", ¿qué digo?**

---

## Fase 3 — Transformación con dbt y agente de consulta

- **¿Qué entendí que antes no entendía?**
- **¿Qué error cometí y cómo lo resolví?**
- **¿Qué decisión de arquitectura cuestioné o cambié, y por qué?**
- **Si me preguntan "explicame esta fase en 30 segundos", ¿qué digo?**

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
