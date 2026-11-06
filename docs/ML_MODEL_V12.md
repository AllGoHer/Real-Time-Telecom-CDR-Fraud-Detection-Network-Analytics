# SentinelStream — Modelo de detección de anomalías ML V12

## 1. Resumen

SentinelStream utiliza Isolation Forest de scikit-learn para identificar patrones anómalos en eventos CDR (Call Detail Records).

V12 es la versión activa del consumidor de inferencia online. El entrenamiento, la calibración y la evaluación del modelo se realizan offline; la inferencia consume eventos desde Kafka y publica las predicciones para su persistencia y visualización.

El sistema detecta anomalías estadísticas. Una anomalía no constituye, por sí sola, una confirmación de fraude.

## 2. Modelo y características

- Algoritmo: `sklearn.ensemble.IsolationForest`.
- Versión: V12.
- Número de estimadores: 300.
- Contamination: 0.05.
- Random state: 42.
- Umbral de decisión: `0.07884560010740427`.

Características utilizadas:

| Característica | Descripción |
|---|---|
| `prior_calls_1m` | Llamadas previas del emisor en el último minuto |
| `prior_calls_5m` | Llamadas previas del emisor en los últimos cinco minutos |
| `prior_calls_15m` | Llamadas previas del emisor en los últimos quince minutos |
| `prior_unique_destinations_24h` | Destinos únicos contactados por el emisor en las últimas 24 horas |

El score se calcula como el negativo de `model.decision_function(X)`. Un valor mayor representa un comportamiento más anómalo. La predicción se determina comparando el score con el umbral configurado.

El score no es una probabilidad calibrada de fraude.

## 3. Entrenamiento y evaluación offline

El modelo se entrenó y evaluó con un conjunto de datos sintético:

- Archivo: `experiments/synthetic_data/cdr_experiment_v12.parquet`.
- Registros: 518,400.
- Emisores: 500.
- Episodios sintéticos: 1,058.
- Separación por emisor para evitar compartir emisores entre entrenamiento, calibración y prueba.

| Conjunto | Emisores | Registros |
|---|---:|---:|
| Entrenamiento | 300 | 310,902 |
| Calibración | 100 | 103,681 |
| Prueba | 100 | 103,817 |

El umbral se seleccionó en calibración maximizando F1, con precisión como criterio de desempate.

### Resultados en el conjunto de prueba

| Métrica | Resultado |
|---|---:|
| Accuracy | 0.9883 |
| Precision | 0.8336 |
| Recall | 0.7031 |
| F1 | 0.7628 |
| PR-AUC | 0.7143 |
| ROC-AUC | 0.9434 |

Matriz de confusión:

| | Predicción normal | Predicción anomalía |
|---|---:|---:|
| Real normal | 100,656 | 389 |
| Real anomalía | 823 | 1,949 |

Estos resultados corresponden exclusivamente a etiquetas y datos sintéticos. No representan métricas de precisión o recall sobre fraude real.

## 4. Artefactos del modelo

Los artefactos principales se encuentran en:

- `experiments/models/isolation_forest_v12_baseline.joblib`
- `experiments/models/isolation_forest_v12_baseline.json`
- `experiments/results/anomaly_evaluation_v12.txt`
- `experiments/results/anomaly_predictions_v12.parquet`

El archivo JSON contiene los metadatos del modelo, las características, el umbral y las métricas de evaluación.

## 5. Inferencia online

El consumidor `src/inference/consumer_v12.py`:

1. Consume eventos CDR desde Kafka.
2. Mantiene el estado necesario para calcular las características temporales.
3. Carga el modelo y los metadatos V12.
4. Calcula el score de anomalía y aplica el umbral.
5. Publica las predicciones en Kafka.

El topic utilizado actualmente es `cdr_ml_predictions_v11` y el grupo de consumidores es `sentinelstream-v11-inference`. Son nombres heredados de la integración anterior; el modelo que realiza la inferencia activa es V12.

El proceso `src/inference/ml_predictions_to_postgres.py` consume las predicciones y las persiste en `public.fraud_ml_predictions`.

Los paneles ML 7, 8 y 9 del dashboard Grafana filtran los registros con `model_version = 'V12'`.

## 6. Validación online observada — 5 de noviembre de 2026

Durante la comprobación operativa de V12 se observó la siguiente muestra en PostgreSQL:

| Resultado | Registros |
|---|---:|
| Predicciones procesadas | 688 |
| Anomalías | 15 |
| Normales | 673 |
| Porcentaje de anomalías | 2.18 % |

Esta muestra confirma que el consumidor V12 produjo predicciones y que estas llegaron a PostgreSQL. También permitió comprobar que el dashboard refleja la versión activa.

Como antecedente, la muestra observada de V11 había presentado 1,074 predicciones y una tasa de anomalías del 100 %. Esa observación motivó la evaluación y sustitución del consumidor activo.

El 2.18 % es una tasa observada de anomalías en esa muestra concreta. No es una tasa de fraude confirmado ni una medición de precisión del modelo.

## 7. Limitaciones y trabajo futuro

- Los datos y las etiquetas utilizados en la evaluación offline son sintéticos.
- No se dispone de etiquetas verificadas de fraude real para calcular precisión y recall operativos.
- El score de Isolation Forest no representa una probabilidad de fraude.
- La tasa de anomalías depende de la distribución del tráfico y del umbral aplicado.
- Se requiere validación con tráfico real etiquetado y seguimiento de cambios en la distribución de características.
- El artefacto entrenado y el consumidor online son componentes separados; el modelo por sí solo no implementa el procesamiento streaming.

## 8. Conclusión

V12 integra un modelo Isolation Forest entrenado offline en un flujo de inferencia online basado en Kafka. La integración se ha comprobado mediante la generación de predicciones, su persistencia en PostgreSQL y su visualización en Grafana.

Los resultados sintéticos permiten evaluar el comportamiento del modelo bajo condiciones controladas. La eficacia para detectar fraude real permanece pendiente de validación con datos reales etiquetados.
