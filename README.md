![SentinelStream - Real-Time Telecom CDR Fraud Detection](docs/images/sentinelstream-cover.png)

# SentinelStream – Real-Time Telecom CDR Fraud Detection & Network Analytics

Plataforma local de ingeniería de datos orientada al procesamiento de registros de llamadas telefónicas (Call Detail Records, CDR), detección experimental de anomalías y análisis de métricas de red mediante tecnologías open source y autohospedadas.

SentinelStream implementa una arquitectura de streaming basada en Apache Kafka y Apache Flink, procesamiento histórico con Polars, persistencia en PostgreSQL y monitorización con Grafana. Incorpora inferencia online con Isolation Forest V12, cuyo entrenamiento y evaluación se realizan offline.


## Arquitectura

### Flujo integral de datos — SentinelStream

La arquitectura separa el procesamiento de eventos en tiempo real, el procesamiento histórico y el ciclo de Machine Learning. Isolation Forest V12 participa en la inferencia online; su entrenamiento y evaluación se realizan offline.

<p align="center">
  <img
    src="docs/images/sentinelstream-architecture.png"
    alt="SentinelStream - Arquitectura integral de datos y Machine Learning V12"
    width="850"
  />
</p>

**Recorrido de los datos**

1. **Ingesta:** el generador Python produce eventos CDR y los publica en Apache Kafka mediante KRaft.
2. **Streaming:** Apache Flink procesa los eventos, genera alertas y calcula métricas por ventanas.
3. **Procesamiento histórico:** los datos Bronze en JSON se validan y transforman con Polars para generar archivos Silver en Parquet.
4. **Machine Learning:** Isolation Forest V12 se entrena y calibra offline. Su artefacto se utiliza en el consumidor de inferencia online, que publica predicciones en Kafka.
5. **Persistencia:** PostgreSQL almacena alertas, métricas agregadas y predicciones ML.
6. **Visualización:** Grafana consulta PostgreSQL para mostrar el comportamiento del sistema y las detecciones de anomalías.

**Nota técnica:** el topic `cdr_ml_predictions_v11` conserva su nombre histórico por compatibilidad; el modelo que realiza la inferencia es V12. El entrenamiento offline y la inferencia online son procesos distintos.

## Stack tecnológico

| Componente | Tecnología | Función |
|---|---|---|
| Generación de datos | Python | Generación de CDR sintéticos |
| Ingesta | Apache Kafka 3.8.0 | Transporte de eventos mediante KRaft |
| Streaming | Apache Flink 1.18.1 / PyFlink | Procesamiento de eventos y métricas |
| Bronze | JSON | Conservación de registros originales |
| Silver | Polars | Limpieza y transformación |
| Formato columnar | Apache Parquet | Almacenamiento de datos procesados |
| Gold | PostgreSQL | Persistencia de métricas |
| Machine Learning | Python / scikit-learn | Detección experimental de anomalías |
| Inferencia online | Isolation Forest V11 | Scoring de eventos consumidos desde Kafka |
| Experimentos offline | Isolation Forest V11 / V12 | Entrenamiento, calibración y evaluación |
| Visualización | Grafana 10.4.2 | Monitorización y dashboards |
| Contenedores | Docker Compose | Entorno local autohospedado |

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
│   │   ├── isolation_forest_v11_baseline.joblib
│   │   ├── isolation_forest_v11_baseline.json
│   │   ├── isolation_forest_v12_baseline.joblib
│   │   └── isolation_forest_v12_baseline.json
│   ├── results/
│   │   ├── anomaly_evaluation_v11.txt
│   │   ├── anomaly_evaluation_v12.txt
│   │   └── rules_evaluation_v1.txt
│   ├── build_cdr_features_v1.py
│   ├── evaluate_rules_v1.py
│   ├── evaluate_v12_production.py
│   ├── export_baseline_v11.py
│   ├── generate_synthetic_cdr_v10.py
│   ├── generate_synthetic_cdr_v12.py
│   ├── train_anomaly_v11.py
│   └── train_anomaly_v12.py
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
│   │   ├── silver_layer.py
│   │   ├── silver_layer_v4.py
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

*Nota: el árbol muestra los componentes principales y no enumera todos los archivos históricos ni los artefactos generados.*

## Procesamiento de datos: arquitectura Medallion

### Bronze – Ingesta de datos crudos

- Generación de registros CDR sintéticos mediante Python.
- Ingesta de eventos a través de Apache Kafka.
- Procesamiento de streaming con Apache Flink y PyFlink.
- Conservación de registros en formato JSON.

### Silver - Procesamiento y transformacion

El procesamiento historico se realiza mediante Polars sobre los archivos JSON
conservados en Bronze. Las salidas se almacenan en formato Apache Parquet.

**Evolucion de las versiones:**

- **V3:** version historica inicial del procesamiento Silver.
- **V4:** incorpora la normalizacion de `is_burst_event` nulo a `false`,
  conserva las columnas de eventos Burst y genera un archivo independiente:
  `silver_data_v4.parquet`.
- V4 procesa los registros Bronze disponibles al momento de su ejecucion.
  Respecto del snapshot utilizado por V3, incorpora 4,502 registros que
  llegaron posteriormente a Bronze; no son registros generados artificialmente
  por la transformacion.

**Transformaciones de Silver V4:**

- Validacion de campos obligatorios y duracion no negativa.
- Deduplicacion por `call_id`.
- Normalizacion de `is_burst_event` nulo a `false`.
- Conservacion de `burst_id` y `burst_call_index` para los eventos Burst.
- Generacion de `duration_minutes`.
- Clasificacion de duracion: `short`, `medium` y `long`.
- Identificacion de llamadas internacionales.
- Escritura del resultado en Apache Parquet mediante Polars.

**Auditoria del resultado V4:**

| Indicador | Resultado |
|---|---:|
| Registros Silver | 380,877 |
| Registros con Burst `false` | 380,710 |
| Registros con Burst `true` | 167 |
| Valores nulos en `is_burst_event` | 0 |
| `call_id` duplicados en Silver | 0 |
| Duraciones negativas | 0 |
| Registros historicos sin campo Burst normalizados a `false` | 374,602 |

La auditoria confirmo la conservacion de los 167 eventos Burst disponibles
en Bronze y la ausencia de duplicados por `call_id` en el resultado Silver.

Los 374,602 registros historicos que no contenian el campo
`is_burst_event` se normalizaron a `false`. Este tratamiento representa
la ausencia del indicador en los registros historicos, no una etiqueta
de fraude ni una clasificacion confirmada de comportamiento normal.

El procesamiento Silver V4 es un flujo historico por lotes. No alimenta
directamente al consumer de inferencia online V11, que consume eventos
desde Kafka de manera independiente.

### Gold – Métricas y persistencia

- Procesamiento de métricas mediante Apache Flink.
- Persistencia en PostgreSQL.
- Visualización mediante Grafana.
- Monitorización de indicadores y tasa de fraude por país.

## Machine Learning: entrenamiento e inferencia

### V11 – Modelo integrado en inferencia online

Se desarrolló un modelo baseline utilizando Isolation Forest de scikit-learn para evaluar la detección de anomalías en registros CDR sintéticos.

**Configuración:**

- Algoritmo: Isolation Forest.
- Número de estimadores: 300.
- Contamination: 0.05.
- Características: 4 variables de comportamiento.
- Semilla aleatoria: 42.
- Selección del umbral mediante conjunto de calibración.

**Partición de datos:**

- 300 llamantes para entrenamiento.
- 100 llamantes para calibración.
- 100 llamantes para prueba.

**Resultados experimentales:**

| Métrica | Resultado |
|---|---:|
| Accuracy | 0.9790 |
| Precision | 0.9647 |
| Recall | 0.9245 |
| F1-score | 0.9441 |
| PR-AUC | 0.8973 |
| ROC-AUC | 0.9609 |
| TP | 355 |
| FP | 13 |
| TN | 1,603 |
| FN | 29 |

También se evaluó una variante ampliada con `is_new_destination_24h`. El baseline obtuvo un F1-score de 0.9441 frente a 0.9318 de la variante ampliada.

**Integración online:**

El artefacto V11 se exporta mediante `export_baseline_v11.py` y es utilizado por `src/inference/consumer_v11.py`.

El flujo:

1. Consume eventos CDR desde Kafka.
2. Mantiene variables de comportamiento del llamante.
3. Calcula el anomaly score.
4. Compara el score con el umbral configurado.
5. Publica predicciones en Kafka.
6. Persiste las predicciones en PostgreSQL mediante `ml_predictions_to_postgres.py`.

**Limitación operativa:** aunque V11 está integrado técnicamente, el tráfico generado mostró una tasa de alertas elevada. Por tanto, no se considera validado para detección operativa de fraude.

### V12 – Experimento offline independiente

V12 amplía el experimento con un conjunto sintético de mayor tamaño y una evaluación independiente por abonado.

**Dataset:**

| Elemento | Cantidad |
|---|---:|
| Registros CDR | 518,400 |
| Abonados | 500 |
| Episodios sintéticos | 1,058 |
| Entrenamiento | 300 abonados |
| Calibración | 100 abonados |
| Prueba | 100 abonados |

**Configuración:**

- Algoritmo: Isolation Forest.
- Estimadores: 300.
- Contamination: 0.05.
- Semilla aleatoria: 42.
- Variables de comportamiento:
  - `prior_calls_1m`
  - `prior_calls_5m`
  - `prior_calls_15m`
  - `prior_unique_destinations_24h`

El umbral se seleccionó utilizando el conjunto de calibración mediante F1 máximo, con precisión como criterio de desempate.

**Resultados en el conjunto de prueba:**

| Métrica | Resultado |
|---|---:|
| Accuracy | 0.9883 |
| Precision | 0.8336 |
| Recall | 0.7031 |
| F1-score | 0.7628 |
| PR-AUC | 0.7143 |
| ROC-AUC | 0.9434 |
| TP | 1,949 |
| FP | 389 |
| TN | 100,656 |
| FN | 823 |

**Artefactos principales:**

- `experiments/generate_synthetic_cdr_v12.py`
- `experiments/train_anomaly_v12.py`
- `experiments/evaluate_v12_production.py`
- `experiments/models/isolation_forest_v12_baseline.joblib`
- `experiments/models/isolation_forest_v12_baseline.json`
- `experiments/results/anomaly_evaluation_v12.txt`
- `experiments/results/anomaly_predictions_v12.parquet` (generado localmente; excluido de Git)

V12 permanece como experimento offline. No reemplaza al modelo V11, no publica predicciones en Kafka y no modifica la persistencia PostgreSQL del flujo online.

### Limitaciones de Machine Learning

- Los registros y las etiquetas utilizados son sintéticos.
- Los resultados no demuestran rendimiento con fraude real.
- Los resultados de V11 y V12 corresponden a experimentos diferentes y no deben interpretarse como una comparación directa de modelos sobre un mismo conjunto de prueba.
- El score de Isolation Forest no representa una probabilidad calibrada de fraude.
- La inferencia V11 requiere ajustes y validación por su elevada tasa de alertas en tráfico generado.
- Los modelos y umbrales necesitan validación con datos representativos antes de cualquier uso operativo.

## Instalación y ejecución

El proyecto utiliza entornos virtuales independientes para evitar conflictos entre dependencias.

Consulta la documentación para los procedimientos completos:

- `docs/EXECUTION_GUIDE.md`: guía de ejecución.
- `docs/TESTING.md`: validaciones y pruebas.
- `docs/TROUBLESHOOTING.md`: resolución de problemas.
- `docs/ARCHITECTURE.md`: arquitectura técnica.
- `docs/DECISIONS.md`: decisiones de diseño.

## Pruebas

Las pruebas automatizadas del consumer V11 se ejecutan con:

```powershell
.\.venv-inference\Scripts\python.exe -m pytest -q tests/test_consumer_v11.py
```

Última validación documentada: 10 pruebas aprobadas.

No se deben considerar aprobadas pruebas adicionales sin ejecutarlas y verificar sus resultados.

## Objetivos técnicos del proyecto

SentinelStream permite demostrar experiencia práctica en:

- Arquitecturas de streaming con Apache Kafka.
- Procesamiento de eventos con Apache Flink y PyFlink.
- Diseño de pipelines de datos Bronze y Silver.
- Procesamiento columnar con Polars y Apache Parquet.
- Persistencia analítica en PostgreSQL.
- Observabilidad y dashboards con Grafana.
- Integración de modelos de Machine Learning con consumidores Kafka.
- Entrenamiento, calibración y evaluación de modelos de anomalías.
- Desarrollo y documentación de sistemas open source autohospedados.

## Estado y alcance

SentinelStream es un proyecto de portafolio orientado a ingeniería de datos, procesamiento de eventos y experimentación con detección de anomalías.

La arquitectura implementa un flujo Kafka/Flink y una ruta de inferencia online V11, junto con experimentos offline V11 y V12. Los resultados de Machine Learning se obtuvieron con datos sintéticos y no constituyen evidencia de detección efectiva de fraude real.

El proyecto continúa sujeto a mejoras de validación, calibración y evaluación con datos representativos.

## Licencia

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.