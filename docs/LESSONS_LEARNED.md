# Lecciones aprendidas — SentinelStream

## 1. Objetivo del proyecto

SentinelStream es un proyecto de portafolio orientado al procesamiento de registros de llamadas telefónicas (CDR), el análisis de red y la exploración de técnicas de detección de anomalías.

## 2. Decisiones técnicas

### Kafka en modo KRaft

Se utiliza Kafka con KRaft, sin ZooKeeper, para simplificar la infraestructura local.

### PyFlink para el procesamiento de streaming

Flink permite desarrollar procesamiento de eventos en tiempo real mediante Python y PyFlink. Es importante distinguir entre tener los servicios iniciados y haber validado correctamente un trabajo de procesamiento.

### Polars para la capa Silver

Polars se utiliza para trabajar con datos tabulares y generar archivos Parquet. Esta elección evita depender de Spark para las transformaciones locales de esta fase.

### PostgreSQL y Grafana

PostgreSQL almacena métricas de la capa Gold y Grafana permite visualizarlas. La disponibilidad de los contenedores no garantiza por sí sola que las consultas y los dashboards funcionen correctamente.

## 3. Problemas técnicos y soluciones aplicadas

### MinIO: dificultades de disponibilidad de la imagen

Durante la configuración inicial se presentaron problemas al descargar la imagen de MinIO desde Docker Hub. Se decidió retirar MinIO de la arquitectura actual para evitar incorporar una dependencia que no era necesaria para continuar con el desarrollo.

### Apache Iceberg: complejidad de dependencias

Se descartó su incorporación en esta fase debido a la complejidad adicional de configuración e integración de dependencias en el entorno local. Su posible utilización queda fuera del alcance actual.

### Apache Spark: compatibilidad en Windows

Se evaluó Spark para las transformaciones de datos, pero los requisitos adicionales de configuración en Windows, incluidos los relacionados con `winutils.exe`, motivaron la elección de Polars para la capa Silver.

### Entornos virtuales de Python

La separación de entornos virtuales permite aislar las dependencias de cada componente y reducir conflictos entre versiones de bibliotecas. El generador, las transformaciones Silver, los experimentos de Machine Learning y la inferencia cuentan con dependencias diferenciadas.

## 4. Separación entre streaming y Machine Learning

El experimento V11 con Isolation Forest y scikit-learn se mantiene como un análisis offline independiente del pipeline principal de streaming.

El modelo utiliza cuatro características de comportamiento de llamadas y trabaja con datos CDR sintéticos. Su puntuación representa un nivel de anomalía y no una probabilidad calibrada de fraude.

Los resultados experimentales permiten evaluar el comportamiento del enfoque bajo los datos generados, pero no constituyen evidencia de detección de fraude real ni demuestran rendimiento en producción.

Esta separación permite experimentar con Machine Learning sin afirmar que el modelo forma parte del flujo de streaming operativo.

## 5. Gestión de datos y reproducibilidad

- Verificar los archivos y las rutas antes de ejecutar transformaciones.

- Revisar los logs cuando un servicio o trabajo presenta errores.

- Comprobar el esquema, los valores nulos y la unicidad de los identificadores.

- Documentar las versiones, los comandos y los resultados observados.

- Evitar sobrescribir archivos de salida durante las pruebas.

- Mantener identificadas las rutas y responsabilidades de cada capa: Bronze almacena registros JSON, Silver genera archivos Parquet mediante Polars y Gold conserva métricas en PostgreSQL.

- Diferenciar las transformaciones históricas de Silver de los trabajos continuos de streaming ejecutados con Flink.

## 6. Seguridad al trabajar con Docker

Antes de ejecutar comandos que reinicien servicios o modifiquen volúmenes, revisar su impacto.

No utilizar `docker compose down -v` como procedimiento rutinario: puede eliminar volúmenes y datos persistentes.

También conviene revisar los scripts de inicio antes de ejecutarlos y confirmar qué servicios y rutas utilizan.

## 7. Limitaciones actuales

- El proyecto depende de la configuración local de Docker y de sus archivos de configuración.

- Los resultados del experimento de anomalías son sintéticos y offline.

- La validación debe basarse en pruebas realmente ejecutadas, no únicamente en la documentación o en el estado de los contenedores.

- Las decisiones técnicas reflejan el alcance actual del proyecto y pueden cambiar en futuras versiones.

## 8. Próximas mejoras

1. Automatizar pruebas reproducibles para validar los datos de Bronze, Silver y Gold.

2. Añadir evidencia verificable de las pruebas funcionales de Kafka y Flink.

3. Documentar ejemplos de consultas y métricas de PostgreSQL y Grafana.

4. Mantener el experimento offline separado hasta que exista una integración de anomalías diseñada, implementada y probada de forma explícita.

5. Incorporar validaciones automatizadas de calidad de datos, incluyendo esquema, campos obligatorios, rangos válidos y duplicidad de identificadores.

6. Mejorar la observabilidad de los trabajos Flink mediante indicadores de estado, errores, reinicios y progreso del procesamiento.

## 9. Conclusión

La principal lección es mantener una arquitectura coherente, validar cada componente por separado y documentar con precisión qué está implementado, qué se ha probado y qué permanece como experimento.
