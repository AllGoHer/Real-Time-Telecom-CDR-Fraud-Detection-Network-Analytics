# Guía de ejecución — SentinelStream

## 1. Objetivo

Esta guía describe cómo revisar, iniciar y verificar el entorno local de SentinelStream en Windows 11 con Docker Desktop y WSL 2.

## 2. Requisitos previos

- Windows 11.

- Docker Desktop iniciado.

- Docker Compose disponible.

- Repositorio clonado localmente.

- PowerShell abierto en la raíz del proyecto.

## 3. Verificar la configuración

Antes de iniciar o modificar servicios, comprueba que Docker esté disponible:

```powershell

docker --version

docker compose version

docker compose ps

```

Revisa el archivo `docker-compose.yml` y confirma qué servicios, puertos, volúmenes y archivos de configuración se utilizarán.

## 4. Preparación e inicio del entorno

El entorno se administra mediante Docker Compose y un script de PowerShell ubicado en `scripts/start_sentinelstream.ps1`.

### 4.1. Configurar las variables de entorno

Crea el archivo `.env` a partir de la plantilla únicamente si todavía no existe:

```powershell
if (!(Test-Path .env)) {
    Copy-Item .env.example .env
    Write-Host "Archivo .env creado."
} else {
    Write-Host "El archivo .env ya existe. No se modificó."
}
```

Completa las variables requeridas en `.env` con tus propios valores. Este archivo contiene configuración local y no debe publicarse en GitHub.

### 4.2. Revisar la configuración

Antes de iniciar el entorno:

1. Comprueba que Docker Desktop esté disponible.
2. Revisa los servicios, puertos, volúmenes y dependencias definidos en `docker-compose.yml`.
3. Comprueba el estado actual mediante `docker compose ps`.
4. Verifica que las variables de entorno requeridas estén configuradas.

### 4.3. Inicio automatizado de SentinelStream

El repositorio incluye `scripts/start_sentinelstream.ps1`, que comprueba y recupera el entorno de SentinelStream.

El script:

1. Comprueba la disponibilidad de Docker.
2. Inicia los contenedores requeridos que estén detenidos, sin recrearlos.
3. Verifica la disponibilidad de Kafka, PostgreSQL y Flink.
4. Comprueba los jobs Bronze, Fraud Alerts y Gold, e inicia los que falten.
5. Comprueba el generador CDR y lo inicia si no está activo.
6. Comprueba el consumidor ML V12 y el sink PostgreSQL, e inicia los que falten.

**Comprobación sin iniciar ni detener procesos:**

```powershell
.\scripts\start_sentinelstream.ps1 -CheckOnly
```

## 5. Verificar los servicios

Consulta el estado de los contenedores:

```powershell

docker compose ps

```

Consulta los registros de un servicio cuando necesites diagnosticar un problema:

```powershell

docker compose logs --tail 100 kafka

docker compose logs --tail 100 flink-jobmanager

docker compose logs --tail 100 flink-taskmanager

docker compose logs --tail 100 postgres

docker compose logs --tail 100 grafana

```

Los nombres de servicio deben coincidir con los definidos en el archivo Compose. Si no coinciden, utiliza los nombres reales de esa configuración.

**Observación:** Que un contenedor aparezca como Up no garantiza que la aplicación esté completamente operativa. Comprueba también los registros y la conectividad entre los componentes antes de considerar validado el flujo de datos.

## 6. Kafka y generación de CDR

Verifica los listeners efectivos en `docker-compose.yml` y en la configuración del productor antes de enviar eventos.

El generador de CDR se ejecuta desde el equipo anfitrión y utiliza el listener publicado de Kafka. Los trabajos Flink utilizan la dirección interna de Kafka dentro de la red Docker.

La ejecución del generador produce nuevos eventos sintéticos, por lo que debe iniciarse únicamente cuando los servicios requeridos estén disponibles.

El generador se ejecuta en el equipo anfitrión mediante Python y utiliza `localhost:9094`. Los trabajos Flink se conectan a Kafka mediante `kafka:9092`, dentro de la red Docker.

## 7. Capas de datos

- **Bronze:** registros JSON sin procesar, conservados a partir de la ingesta de eventos mediante Flink.
- **Silver V5:** snapshot histórico por lotes procesado con Polars y almacenado en Apache Parquet.
- **Gold:** métricas calculadas mediante Flink y persistidas en PostgreSQL.

### 7.1. Silver V5: procesamiento histórico

El proceso más reciente auditado se encuentra en `src/polars_jobs/silver_layer_v5.py`.

- **Entrada:** archivos JSON históricos conservados en `data/bronze_data/`.
- **Procesamiento:** lectura, validación, limpieza, deduplicación y enriquecimiento mediante Polars.
- **Salida:** `data/silver_data/silver_data_v5.parquet`.
- **Modalidad:** ejecución batch que genera un snapshot; no es un flujo continuo.

Silver V5 es independiente de los trabajos Flink. No alimenta directamente las métricas Gold ni el consumer de inferencia online V12. La ejecución automatizada de `scripts/start_sentinelstream.ps1` no debe interpretarse como ejecución de este proceso batch.

Las versiones anteriores (`silver_layer.py` y `silver_layer_v4.py`) se conservan como parte de la evolución histórica del proyecto.

Antes de ejecutar un proceso que escriba datos, revisa sus rutas de salida y el modo de escritura. Algunos procesos pueden reemplazar archivos existentes. No ejecutes transformaciones sobre datos existentes sin confirmar previamente qué archivos o tablas modificarán.

## 8. Grafana

Accede a Grafana mediante el puerto publicado en la configuración de Docker Compose. Consulta `docker-compose.yml` y los archivos de aprovisionamiento para confirmar la dirección, las credenciales configuradas y los paneles disponibles.

No incluyas contraseñas ni secretos en capturas, documentación pública o repositorios.

La instancia de Grafana utiliza el puerto 3000 cuando está publicado según la configuración actual de Docker Compose.

## 9. Machine Learning: inferencia online V12 y evaluación offline

SentinelStream utiliza Isolation Forest V12 como versión activa para la inferencia online. El entrenamiento y la evaluación offline son procesos independientes.

### 9.1. Inferencia online V12

La inferencia online está formada por dos procesos Python separados. `scripts/start_sentinelstream.ps1` comprueba su estado y recupera los procesos que no están activos. Docker Compose no inicia estos procesos Python.

- Consume eventos desde Kafka, tópico `cdr_events`.
- Carga el modelo Isolation Forest V12 y sus metadatos.
- Mantiene características de comportamiento de los llamantes mediante SQLite.
- Calcula puntuaciones de anomalía y publica predicciones en `cdr_ml_predictions_v11`.
- Utiliza el grupo `sentinelstream-v11-inference`.
- Se conecta a Kafka mediante `localhost:9094`.

El tópico y el grupo conservan nombres históricos de V11 por compatibilidad. Esto no significa que V11 sea la versión activa.

### 9.2. Persistencia de predicciones en PostgreSQL

Archivo: `src/inference/ml_predictions_to_postgres.py`

- Consume el tópico `cdr_ml_predictions_v11`.
- Utiliza el grupo `sentinelstream-ml-postgres-sink`.
- Persiste las predicciones en `public.fraud_ml_predictions`.
- Obtiene las credenciales de PostgreSQL desde `.env`.
- Se conecta a PostgreSQL en `localhost:5433`.

Este proceso es independiente del consumidor V12. Si no está iniciado, las predicciones no se incorporarán a PostgreSQL hasta que el sink las consuma.

### 9.3. Comprobación y recuperación de los procesos online

Los procesos de inferencia V12 y persistencia en PostgreSQL son gestionados
por `scripts/start_sentinelstream.ps1`.

**Comprobar el estado sin iniciar ni detener procesos:**

```powershell
.\scripts\start_sentinelstream.ps1 -CheckOnly
```

### 9.4. Entrenamiento y evaluación offline V12

Los procesos offline son independientes de la inferencia online:

- Entrenamiento: `experiments/train_anomaly_v12.py`.
- Evaluación: `experiments/evaluate_v12_production.py`.
- Modelo: `experiments/models/isolation_forest_v12_baseline.joblib`.
- Metadatos: `experiments/models/isolation_forest_v12_baseline.json`.

El entrenamiento y la evaluación utilizan datos sintéticos. Sus resultados permiten evaluar el funcionamiento técnico, pero no demuestran precisión frente a fraude real.

### 9.5. V11 como referencia histórica

V11 se conserva como versión histórica. V12 es la versión activa de inferencia online.

Se mantienen identificadores heredados por compatibilidad:

- Tópico: `cdr_ml_predictions_v11`.
- Grupo del consumidor: `sentinelstream-v11-inference`.
- Estado SQLite: `src/inference/state_v11.sqlite3`.

No renombrar estos identificadores sin planificar una migración coordinada.

### 9.6. Alcance y limitaciones

- Isolation Forest identifica patrones anómalos; una anomalía no confirma un fraude.
- La puntuación de anomalía no es una probabilidad calibrada de fraude.
- Las etiquetas y los datos sintéticos no sustituyen datos reales verificados.
- No se dispone de etiquetas verificadas de fraude real para afirmar precisión operativa.
- Se requiere validación con datos representativos antes de utilizar el modelo en decisiones reales.


## 10. Detención y protección de datos

No utilices `docker compose down -v` como método rutinario de detención: la opción `-v` puede eliminar volúmenes administrados por Compose.

Antes de detener, recrear o eliminar contenedores, identifica los volúmenes asociados y confirma dónde persisten los datos. No borres carpetas de Bronze, Silver, Gold o resultados experimentales sin una copia de seguridad.

## 11. Diagnóstico

Si un servicio no inicia:

1. Comprueba el estado con `docker compose ps`.

2. Consulta sus registros con `docker compose logs`.

3. Verifica puertos, variables de entorno y dependencias.

4. Revisa el archivo `docs/TROUBLESHOOTING.md`.

5. No elimines volúmenes ni reconstruyas todos los servicios sin identificar primero la causa.

## 12. Alcance

Esta guía describe el procedimiento de operación local. La disponibilidad efectiva de cada flujo debe confirmarse mediante los servicios, scripts y configuraciones presentes en el repositorio.
