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

El repositorio incluye `scripts/start_sentinelstream.ps1`, que coordina las siguientes operaciones:

1. Inicia los servicios definidos en Docker Compose mediante `docker compose up -d`.
2. Intenta comprobar la disponibilidad de Flink durante un máximo aproximado de 60 segundos.
3. Consulta los trabajos Flink activos e intenta iniciar Bronze, Fraud Alerts y Gold cuando no encuentra sus nombres en la lista de trabajos en ejecución.
4. Busca procesos de Windows cuya línea de comandos contenga `cdr_generator.py`.
5. Si no encuentra ninguno, inicia el generador utilizando `.venv-producer`.

**Consideraciones:**

- La comprobación de disponibilidad se realiza sobre Flink; no garantiza que Kafka y PostgreSQL estén listos.
- La detección del generador se basa en el nombre del archivo y no verifica que el proceso pertenezca necesariamente a esta instancia del proyecto.
- El inicio del generador no confirma que haya establecido correctamente la conexión con Kafka.
- `docker compose up -d` puede crear o recrear contenedores según el estado y la configuración del entorno.
- El script puede enviar trabajos a Flink y producir nuevos eventos CDR sintéticos.


Importante: este comando inicia contenedores, puede enviar trabajos a Flink y puede arrancar el generador de CDR. No es un comando de comprobación. Ejecútalo únicamente después de revisar la configuración y confirmar que deseas iniciar el entorno.

```powershell
.\scripts\start_sentinelstream.ps1
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

- **Bronze:** registros JSON sin procesar.

- **Silver:** datos transformados con Polars y almacenados en Parquet.

- **Gold:** métricas disponibles en PostgreSQL.

El procesamiento Silver con Polars es una operación histórica independiente. No se ejecuta como una etapa continua dentro de los trabajos Flink.

Antes de ejecutar un trabajo que escriba datos, revisa sus rutas de salida y el modo de escritura. Algunos trabajos pueden reemplazar archivos existentes.

No ejecutes procesos de transformación sobre datos existentes sin confirmar previamente qué archivos o tablas modificarán.

## 8. Grafana

Accede a Grafana mediante el puerto publicado en la configuración de Docker Compose. Consulta `docker-compose.yml` y los archivos de aprovisionamiento para confirmar la dirección, las credenciales configuradas y los paneles disponibles.

No incluyas contraseñas ni secretos en capturas, documentación pública o repositorios.

La instancia de Grafana utiliza el puerto 3000 cuando está publicado según la configuración actual de Docker Compose.

## 9. Machine Learning: inferencia online V11 y experimento offline V12

SentinelStream mantiene dos flujos de Machine Learning independientes. V11 corresponde a la inferencia online conectada a Kafka. V12 es un experimento offline y no está integrado en el consumidor online.

### 9.1. Inferencia online V11

1. `consumer_v11.py` consume eventos del tópico Kafka `cdr_events`.
2. Ejecuta inferencia con `experiments/models/isolation_forest_v11_baseline.joblib`.
3. Publica predicciones en `cdr_ml_predictions_v11`.
4. `ml_predictions_to_postgres.py` almacena las predicciones en `public.fraud_ml_predictions`.

**Requisitos:** Kafka y PostgreSQL activos, generador CDR en ejecución, modelo y metadatos V11 disponibles, entorno `src/inference/venv` instalado y `.env` configurado.

**Ejecución:** abre dos terminales PowerShell desde la raíz del proyecto.

Terminal 1: persistencia en PostgreSQL.

```powershell
.\src\inference\venv\Scripts\python.exe .\src\inference\ml_predictions_to_postgres.py
```

Terminal 2: inferencia V11.

```powershell
.\src\inference\venv\Scripts\python.exe .\src\inference\consumer_v11.py
```

Mantén ambos procesos activos. Para detenerlos utiliza `Ctrl + C` en cada terminal. No inicies instancias duplicadas del consumidor sin revisar el grupo Kafka y su estado SQLite.

Conserva `src/inference/state_v11.sqlite3` y sus archivos auxiliares `-shm` y `-wal`.

**Verificación de predicciones almacenadas:**

```powershell
docker exec postgres-fraud psql -U fraud_user -d fraud_detection -c "SELECT COUNT(*) AS total_predicciones FROM public.fraud_ml_predictions;"
```

### 9.2. Experimento offline V12

V12 es un experimento independiente de entrenamiento y evaluación con datos sintéticos. No sustituye al consumidor V11 ni está conectado al flujo online de Kafka.

Sus métricas representan resultados experimentales sobre datos sintéticos; no deben interpretarse como una tasa de detección de fraude real ni como evidencia de rendimiento en producción.

Antes de volver a ejecutar experimentos, revisa los archivos de salida para evitar sobrescribir resultados existentes.

### 9.3. Alcance y limitaciones

- V11 está implementado como flujo de inferencia online, pero sus resultados son experimentales y dependen de la distribución de los eventos generados.
- V12 corresponde exclusivamente a evaluación offline.
- Los datos utilizados en las pruebas son sintéticos.
- Una predicción o puntuación del modelo no equivale por sí misma a fraude confirmado.
- El sistema no debe presentarse como validado con tráfico real de una operadora.

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
