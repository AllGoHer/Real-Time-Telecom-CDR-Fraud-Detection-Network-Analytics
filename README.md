# SentinelStream – Real-Time Telecom CDR Fraud Detection & Network Analytics

Plataforma local de ingeniería de datos para procesar registros de llamadas telefónicas (Call Detail Records, CDR), generar alertas de fraude y analizar métricas de red mediante tecnologías open source y autohospedadas.

El proyecto implementa un pipeline de streaming con Apache Kafka y Apache Flink, procesamiento de datos históricos con Polars y almacenamiento de métricas en PostgreSQL para su visualización en Grafana.

## Arquitectura

```text
                    CDR Generator
                      (Python)
                         |
                         v
                   Apache Kafka
                   (KRaft mode)
                         |
             +-----------+-----------+
             |                       |
             v                       v
      Apache Flink / PyFlink    ML Inference V11
             |                  (Python / sklearn)
        +----+----+                    |
        |         |                    v
        v         v              Kafka Predictions
     Bronze    Alertas                 |
      JSON   y métricas                v
                  |              ML Persistence
                  |              (Python Consumer)
                  |                    |
                  +----------+---------+
                             |
                             v
                         PostgreSQL
                         Gold: Metrics
                         ML Predictions
                             |
                             v
                           Grafana


       PROCESAMIENTO HISTÓRICO INDEPENDIENTE

          Bronze: JSON existente
                  |
                  v
                Polars
                  |
                  v
           Silver: Parquet


       ENTRENAMIENTO Y EVALUACIÓN OFFLINE

              CDR sintéticos
                    |
                    v
             Feature Engineering
                    |
                    v
              Isolation Forest
                    |
                    v
            Calibración del umbral
                    |
                    v
            Evaluación del modelo
                    |
                    v
             Modelo V11 y metadatos
```
## Stack tecnológico

| Componente | Tecnología | Función |
|---|---|---|
| Generación de datos | Python | Generación de registros CDR sintéticos |
| Ingesta | Apache Kafka 3.8.0 | Transporte de eventos mediante KRaft, sin ZooKeeper |
| Streaming | Apache Flink 1.18.1 / PyFlink | Procesamiento de eventos, reglas de alerta y métricas en tiempo real. |
| Capa Bronze | JSON | Conservación de registros CDR crudos |
| Capa Silver | Polars | Limpieza, validación y transformación de datos |
| Formato columnar | Apache Parquet | Almacenamiento de datos procesados |
| Capa Gold | PostgreSQL | Persistencia de métricas y resultados agregados |
| Visualización | Grafana 10.4.2 | Monitorización de métricas y alertas |
| Machine Learning | Python / scikit-learn / Isolation Forest V11 | Entrenamiento offline e inferencia en línea mediante consumidores Kafka |
| Contenedores | Docker Compose | Despliegue del entorno local |

## Organización del proyecto

```text
telecom-cdr-fraud-detection/
│
├── docker/
│   └── grafana/
│       └── provisioning/
│           ├── dashboards/
│           └── datasources/
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── DECISIONS.md
│   ├── EXECUTION_GUIDE.md
│   ├── LESSONS_LEARNED.md
│   ├── TESTING.md
│   └── TROUBLESHOOTING.md
│
├── experiments/
│   ├── models/
│   ├── results/
│   ├── synthetic_data/
│   ├── build_cdr_features_v1.py
│   ├── evaluate_rules_v1.py
│   ├── export_baseline_v11.py
│   ├── generate_synthetic_cdr_v10.py
│   └── train_anomaly_v11.py
│
├── scripts/
│   ├── init_postgres.sql
│   └── start_sentinelstream.ps1
│
├── src/
│   ├── flink_jobs/
│   │   ├── bronze_ingestion.py
│   │   ├── fraud_alerts.py
│   │   └── gold_layer.py
│   ├── inference/
│   │   ├── consumer_v11.py
│   │   └── ml_predictions_to_postgres.py
│   ├── polars_jobs/
│   │   └── silver_layer.py
│   └── producers/
│       ├── cdr_generator.py
│       └── requirements.txt
│
├── tests/
│   ├── README.md
│   └── test_consumer_v11.py
│
├── requirements/
│   ├── dev.txt
│   ├── inference.txt
│   ├── ml-experiments.txt
│   ├── producer.txt
│   └── silver.txt
│
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Dockerfile.flink
├── LICENSE
└── README.md

```

## Procesamiento de datos: arquitectura Medallón

### Bronze – Ingesta de datos crudos

- Generación de registros CDR sintéticos mediante Python.
- Ingesta de eventos a través de Apache Kafka.
- Procesamiento de streaming con Apache Flink y PyFlink.
- Conservación de los registros en formato JSON.

### Silver – Limpieza y transformación

- Procesamiento local mediante Polars.
- Lectura de registros JSON.
- Validación de campos obligatorios.
- Validación de duración de llamadas.
- Control de duplicados mediante `call_id`.
- Enriquecimiento con duración en minutos, clasificación del tipo de llamada y condición internacional.
- Generación de archivos Parquet.

* El procesamiento Silver mediante Polars se ejecuta como un trabajo histórico independiente del procesamiento de eventos en streaming. No debe interpretarse como una etapa de ejecución continua dentro de Flink.


### Gold – Métricas y monitorización

- Procesamiento de métricas mediante Apache Flink.
- Persistencia en PostgreSQL.
- Visualización mediante Grafana.
- Monitorización de indicadores y tasa de fraude por país.

## Entrenamiento y evaluación offline del modelo V11

Se desarrolló un experimento independiente utilizando Isolation Forest de scikit-learn para evaluar la detección de anomalías en registros CDR sintéticos.


**Configuración del modelo baseline:**

- Algoritmo: Isolation Forest.
- Número de estimadores: 300.
- Contaminación: 0.05.
- Características: 4 variables de comportamiento de llamadas.
- Semilla aleatoria: 42.
- Selección del umbral utilizando el conjunto de calibración.

  **Estrategia de partición de datos:**

La evaluación utiliza una partición por llamante (*caller-based split*), distribuyendo los datos en tres conjuntos independientes: 300 llamantes para entrenamiento, 100 para calibración y 100 para prueba. El umbral de clasificación se selecciona exclusivamente utilizando el conjunto de calibración. El conjunto de prueba se reserva para medir el rendimiento final del modelo.

**Evaluación sobre el conjunto de prueba:**

| Métrica | Resultado |
|---|---:|
| Accuracy | 0.9790 |
| Precision | 0.9647 |
| Recall | 0.9245 |
| F1-score | 0.9441 |
| PR-AUC | 0.8973 |
| ROC-AUC | 0.9609 |
| Verdaderos positivos (TP) | 355 |
| Falsos positivos (FP) | 13 |
| Verdaderos negativos (TN) | 1,603 |
| Falsos negativos (FN) | 29 |

**Comparación experimental**

También se evaluó una variante ampliada con la característica `is_new_destination_24h`. El modelo baseline obtuvo un F1-score de 0.9441 frente a 0.9318 de la variante ampliada, por lo que se conservó el baseline como referencia.

El modelo V11 y sus metadatos se exportan offline mediante `export_baseline_v11.py`. El artefacto resultante es utilizado por `src/inference/consumer_v11.py` para realizar inferencia sobre eventos recibidos desde Kafka.

**Limitaciones del experimento:**

- Los registros y las etiquetas utilizados son sintéticos.
- Los resultados no demuestran rendimiento con fraude real.
- La inferencia V11 está integrada técnicamente en el flujo Kafka, pero el tráfico generado mostró una tasa de alertas elevada; por tanto, el modelo no está validado para uso operativo.
- El score de Isolation Forest no representa una probabilidad calibrada de fraude.

## Instalación de dependencias

El proyecto utiliza entornos virtuales independientes para evitar conflictos entre las dependencias de cada componente.

### Productor Kafka

```powershell
py -3.14 -m venv .venv-producer
.\.venv-producer\Scripts\python.exe -m pip install -r requirements\producer.txt
```

### Procesamiento Silver con Polars

```powershell
py -3.14 -m venv .venv-silver
.\.venv-silver\Scripts\python.exe -m pip install -r requirements\silver.txt
```

### Experimentos de Machine Learning

```powershell
py -3.14 -m venv .venv-ml
.\.venv-ml\Scripts\python.exe -m pip install -r requirements\ml-experiments.txt
```

### Inferencia y pruebas

```powershell
py -3.14 -m venv .venv-inference
.\.venv-inference\Scripts\python.exe -m pip install -r requirements\inference.txt
.\.venv-inference\Scripts\python.exe -m pip install -r requirements\dev.txt
```

## Pruebas

El proyecto incluye pruebas automatizadas para el consumer de inferencia V11.

Para ejecutarlas desde PowerShell:

```powershell
.\.venv-inference\Scripts\python.exe -m pytest -q tests/test_consumer_v11.py
```

Última validación registrada: **10 pruebas superadas**.

## Requisitos

- Windows 11.
- Docker Desktop con WSL 2.
- Python 3.14.
- Git.
- Docker Compose.

Las credenciales y variables de entorno se administran mediante `.env`. El archivo `.env.example` proporciona una plantilla sin credenciales reales.

### Configuración de variables de entorno

Antes de iniciar los servicios, crea el archivo `.env` a partir de la plantilla:

```powershell
if (!(Test-Path .env)) {
    Copy-Item .env.example .env
    Write-Host "Archivo .env creado."
} else {
    Write-Host "El archivo .env ya existe. No se modificó."
}
```

## Seguridad y configuración

- No publicar el archivo `.env`.
- Mantener las credenciales fuera del código fuente.
- Utilizar `.env.example` como plantilla de configuración.
- Verificar que `.env` esté excluido mediante `.gitignore`.

## Alcance y limitaciones

SentinelStream es un proyecto de portafolio técnico orientado a demostrar conocimientos de ingeniería de datos, procesamiento de streaming, arquitectura Medallón, analítica y experimentación con Machine Learning.

El proyecto incorpora un flujo de inferencia online V11 que consume eventos desde Kafka, calcula puntuaciones de anomalía con un modelo Isolation Forest y publica las predicciones para su persistencia en PostgreSQL. La integración técnica de la inferencia está implementada mediante los consumers de `src/inference/`.

El entrenamiento, la calibración del umbral y la evaluación del modelo se realizan offline con datos sintéticos. Las métricas de evaluación no demuestran capacidad de detección de fraude real. Además, las pruebas con tráfico generado en ejecución mostraron una tasa de alertas V11 elevada, por lo que el modelo y su umbral requieren validación y calibración con datos representativos antes de cualquier uso operativo.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
