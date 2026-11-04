# Decisiones de Arquitectura — SentinelStream

## 1. ¿Por qué Kafka con KRaft y no ZooKeeper?

**Decisión:** Utilizar Kafka en modo KRaft, disponible en Kafka 3.x.

**Justificación:** KRaft permite administrar los metadatos de Kafka sin depender de un clúster separado de ZooKeeper. Esto reduce la cantidad de componentes que deben configurarse y mantenerse en el entorno local del proyecto.

## 2. ¿Por qué Apache Flink y PyFlink para el procesamiento en streaming?

**Decisión:** Utilizar Apache Flink 1.18.1 con PyFlink para procesar los eventos CDR consumidos desde Kafka.

**Justificación:** Flink permite implementar procesamiento de eventos en streaming, aplicar reglas de detección y calcular métricas mediante trabajos independientes. PyFlink permite desarrollar estos trabajos utilizando Python, manteniendo la lógica de procesamiento alineada con el lenguaje utilizado en el proyecto.

## 3. ¿Por qué Docker Desktop con WSL 2 en Windows 11?

**Decisión:** Ejecutar los servicios del proyecto mediante Docker Desktop con el backend WSL 2.

**Justificación:** Permite ejecutar los componentes del entorno de datos en contenedores y facilita la reproducción del entorno de desarrollo en Windows. El rendimiento real depende de la configuración de Docker, los volúmenes y los recursos disponibles.

## 4. ¿Por qué PostgreSQL para la capa Gold?

**Decisión:** Utilizar PostgreSQL como almacenamiento relacional de las métricas agregadas de la capa Gold.

**Justificación:** PostgreSQL permite persistir métricas estructuradas generadas por los trabajos de Flink y consultarlas desde Grafana. Su uso proporciona una separación clara entre el procesamiento de eventos y la consulta de indicadores para visualización.

## 5. ¿Por qué Polars para la capa Silver?

**Decisión:** Utilizar Polars para transformar los datos de Bronze y generar archivos Parquet en Silver.

**Justificación:** Polars permite realizar transformaciones tabulares y generar archivos Parquet con una dependencia de ejecución más ligera para el entorno local. Se eligió para evitar incorporar Apache Spark y sus requisitos adicionales de configuración en Windows 11, incluidos los problemas de compatibilidad relacionados con winutils.exe. La decisión mantiene la capa Silver independiente de los trabajos de streaming de Flink.

## 6. ¿Por qué Grafana para la visualización?

**Decisión:** Utilizar Grafana con PostgreSQL como fuente de datos para los paneles del proyecto.

**Justificación:** Grafana permite construir paneles de monitoreo y consultar las métricas almacenadas en PostgreSQL. Se eligió para visualizar los indicadores del proyecto sin incorporar una plataforma adicional de analítica como Apache Superset.

## 7. ¿Por qué separar el procesamiento en streaming del experimento de anomalías?

**Decisión:** Mantener los trabajos de streaming con Kafka y Flink separados del experimento offline de detección de anomalías con scikit-learn.

**Justificación:** El experimento con datos sintéticos permite evaluar un enfoque de detección sin afirmar que el modelo está integrado en el flujo de producción. Los resultados experimentales no demuestran por sí solos la detección de fraude real.

## 8. ¿Por qué mantener el proyecto local y de código abierto?

**Decisión:** Utilizar componentes de código abierto ejecutados localmente mediante Docker.

**Justificación:** El entorno permite practicar y demostrar conceptos de ingeniería de datos y procesamiento en streaming sin depender de servicios de nube.

## 9. ¿Por qué no se incorporaron MinIO, Apache Iceberg y dbt?

**Decisión:** Mantener el almacenamiento local mediante archivos JSON y Parquet, sin incorporar MinIO, Apache Iceberg ni dbt en la arquitectura actual.

**Justificación:** Se priorizó una arquitectura local con menos dependencias y una configuración más sencilla. MinIO no se incorporó debido a problemas de descarga de su imagen desde Docker Hub durante el desarrollo. Apache Iceberg no se incluyó porque no era necesario para los objetivos actuales del proyecto. dbt tampoco se incorporó porque las transformaciones implementadas se realizan mediante los trabajos de Flink y Polars.