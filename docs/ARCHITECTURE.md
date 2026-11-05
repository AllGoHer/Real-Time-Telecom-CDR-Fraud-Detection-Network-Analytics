# Arquitectura de SentinelStream

## 1. Objetivo

SentinelStream es un proyecto local de ingeniería de datos orientado al procesamiento de registros de llamadas telefónicas (CDR), la generación de alertas y la visualización de métricas de red utilizando tecnologías open source.

## 2. Arquitectura general

### Pipeline principal de streaming

```text
                         SENTINELSTREAM
                      ARQUITECTURA GENERAL
                              |
              +---------------+----------------+
              |               |                |
              v               v                v
       PIPELINE DE       INFERENCIA ML    PROCESAMIENTO
        STREAMING           ONLINE          HISTÓRICO
              |               |                |
       GENERADOR CDR          |           BRONZE: JSON
          Python              |                |
              |               |                v
              v               |              POLARS
        APACHE KAFKA           |                |
         MODO KRAFT            |                v
          /       \            |           SILVER: PARQUET
         v         v           |
      APACHE    CONSUMER V11   |
      FLINK     ISOLATION      |
      PYFLINK   FOREST         |
       /  \         |          |
      v    v        v          |
   BRONZE ALERTAS  KAFKA       |
    JSON Y MÉTRICAS PREDICCIONES
           |        |
           |        v
           |    CONSUMER DE
           |    PERSISTENCIA
           |        |
           v        v
        POSTGRESQL
        GOLD + PREDICCIONES
              |
              v
           GRAFANA


       ENTRENAMIENTO Y EVALUACIÓN OFFLINE
                         |
                         v
               DATOS CDR SINTÉTICOS
                         |
                         v
                INGENIERÍA DE FEATURES
                         |
                         v
                ISOLATION FOREST
                  scikit-learn
                         |
                         v
               CALIBRACIÓN Y EVALUACIÓN
                         |
                         v
                 MODELOS V11 (ONLINE) Y V12 (OFFLINE)
```

El diagrama representa las capas y los componentes lógicos del proyecto. Las rutas concretas de escritura y lectura dependen de la configuración de los trabajos y servicios.

## 3. Componentes

| Componente | Tecnología | Responsabilidad |
|---|---|---|
| Generación | Python | Producir registros CDR sintéticos |
| Ingesta | Apache Kafka 3.8.0 con KRaft | Transportar eventos sin ZooKeeper |
| Streaming | Apache Flink 1.18.1 / PyFlink | Procesar eventos, ejecutar reglas de alerta y calcular métricas en tiempo real |
| Bronze | JSON | Almacenar los registros CDR originales generados y procesados por el flujo de ingesta streaming |
| Silver | Polars y Apache Parquet | Limpiar, validar y transformar registros históricos mediante procesamiento independiente |
| Gold | PostgreSQL | Almacenar métricas para consultas |
| Monitorización | Grafana 10.4.2 | Visualizar métricas mediante paneles |
| Inferencia ML | Python / scikit-learn / Isolation Forest V11 | Consumir eventos CDR desde Kafka, calcular puntuaciones de anomalía y publicar predicciones en Kafka |
| Persistencia ML | Python / Kafka / PostgreSQL | Consumir predicciones y almacenarlas en `fraud_ml_predictions` |
| Entrenamiento ML | scikit-learn / Isolation Forest | Entrenar, calibrar y evaluar el modelo offline con datos sintéticos |

## 4. Organización de datos

### Bronze

Contiene los registros CDR originales en formato JSON, recibidos y almacenados por el pipeline de ingesta streaming.

### Silver

Utiliza Polars para procesar registros históricos en JSON y generar archivos Parquet.

Las transformaciones incluyen:

- Validación de campos obligatorios.
- Control de valores nulos.
- Validación de duración de llamadas.
- Eliminación de duplicados mediante `call_id`.
- Generación de atributos derivados.

Este procesamiento se ejecuta como un trabajo independiente y no forma parte del flujo continuo de Flink.

### Gold

Utiliza PostgreSQL para almacenar métricas agregadas y facilitar consultas y visualización mediante Grafana.

Los trabajos de Apache Flink calculan y persisten las métricas correspondientes al pipeline principal.

La implementación exacta de cada flujo debe verificarse en los scripts y la configuración del repositorio.

## 5. Kafka y conectividad

Kafka funciona en modo KRaft, sin ZooKeeper. La configuración del proyecto distingue entre las conexiones internas de los contenedores y la conexión desde el equipo anfitrión.

- Dirección interna para los servicios Docker: `kafka:9092`.

- Dirección publicada para clientes en el equipo anfitrión: `localhost:9094`.

Estas direcciones deben coincidir con los listeners y puertos definidos en la configuración activa de Docker Compose.

## 6. Inferencia online de Machine Learning

El proyecto implementa un flujo de inferencia online mediante dos procesos Python independientes.

### Consumer de inferencia V11

Archivo: `src/inference/consumer_v11.py`

Responsabilidades:

- Consumir registros CDR desde Kafka.
- Mantener características de comportamiento por llamante.
- Construir las variables requeridas por el modelo.
- Cargar el artefacto Isolation Forest V11.
- Calcular la puntuación de anomalía.
- Comparar la puntuación con el umbral configurado.
- Publicar las predicciones en Kafka.

El modelo V11 utiliza cuatro características:

- `prior_calls_1m`
- `prior_calls_5m`
- `prior_calls_15m`
- `prior_unique_destinations_24h`

La puntuación se calcula como el negativo de `decision_function`. Una puntuación mayor representa mayor anomalía según el modelo; no equivale a una probabilidad calibrada de fraude.

### Consumer de persistencia

Archivo: `src/inference/ml_predictions_to_postgres.py`

Consume las predicciones publicadas en Kafka y las persiste en PostgreSQL, en la tabla `fraud_ml_predictions`.

### Limitaciones

- El entrenamiento y la calibración V11 utilizan datos sintéticos.
- Las métricas offline no demuestran detección de fraude real.
- Las pruebas con tráfico generado mostraron una tasa de alertas V11 elevada.
- El modelo y su umbral requieren validación con datos representativos antes de cualquier uso operativo.


### Experimento offline V12

Archivos principales:

- Entrenamiento: `experiments/train_anomaly_v12.py`
- Evaluación comparativa: `experiments/evaluate_v12_production.py`
- Modelo: `experiments/models/isolation_forest_v12_baseline.joblib`
- Metadatos: `experiments/models/isolation_forest_v12_baseline.json`
- Resultados: `experiments/results/anomaly_evaluation_v12.txt`

V12 es un experimento independiente del consumidor online V11. Entrena un modelo
Isolation Forest con un dataset sintético de 518,400 llamadas y 500 abonados.

La partición se realiza por abonado, evitando que un mismo abonado aparezca en
más de un conjunto:

- Entrenamiento: 300 abonados y 310,902 llamadas.
- Calibración: 100 abonados y 103,681 llamadas.
- Prueba: 100 abonados y 103,817 llamadas.

El modelo utiliza 300 estimadores, `contamination=0.05` y `random_state=42`.
El umbral seleccionado en calibración es `0.07884560`, maximizando F1 y usando
precision como criterio de desempate.

Resultados sobre el conjunto de prueba:

| Métrica | Resultado |
|---|---:|
| Accuracy | 0.9883 |
| Precision | 0.8336 |
| Recall | 0.7031 |
| F1 | 0.7628 |
| PR-AUC | 0.7143 |
| ROC-AUC | 0.9434 |

Matriz de confusión: TN=100,656; FP=389; FN=823; TP=1,949.

#### Comparación de V12 con registros evaluados por V11

El script `experiments/evaluate_v12_production.py` consulta registros con estado
`scored` en `public.fraud_ml_predictions`, carga el modelo V12 y calcula sus
predicciones sobre las características almacenadas para comparar ambos modelos.

Esta evaluación es comparativa y de solo lectura: calcula tasas de alerta,
acuerdo entre decisiones y distribuciones de scores y características. No
actualiza PostgreSQL, no publica predicciones en Kafka y no sustituye al
consumidor V11.

La muestra consultada no dispone de etiquetas verificadas de fraude; por tanto,
esta comparación no permite calcular precisión o recall reales ni demostrar
rendimiento en producción.

V12 permanece como artefacto experimental offline y no está conectado al
consumidor de inferencia online V11.

## 7. Entorno de ejecución

El proyecto se ejecuta localmente en Windows 11 mediante Docker Desktop y WSL 2. Los principales servicios de infraestructura utilizan Apache Kafka 3.8.0 en modo KRaft, Apache Flink 1.18.1, PostgreSQL y Grafana 10.4.2.

El generador CDR utiliza Python desde el equipo anfitrión mediante un entorno virtual independiente.

Antes de ejecutar trabajos que escriban en archivos Parquet o tablas de PostgreSQL, se deben revisar las rutas de salida y las operaciones de escritura para evitar sobrescribir datos existentes.

## 8. Limitaciones

- Los CDR utilizados son sintéticos.

- Los resultados del modelo de anomalías no se pueden extrapolar directamente a redes de telecomunicaciones reales.

- La arquitectura documentada debe mantenerse alineada con la configuración efectiva del repositorio.

- La inferencia V11 está integrada técnicamente en el flujo Kafka, pero su rendimiento no está validado para detección de fraude real ni para uso operativo.
- La presencia de un componente en este documento no sustituye la verificación de que su flujo esté implementado y operativo.
