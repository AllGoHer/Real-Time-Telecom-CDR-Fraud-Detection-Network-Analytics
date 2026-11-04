# Pruebas y validación — SentinelStream

## 1. Objetivo

Documentar las comprobaciones para validar la configuración y el funcionamiento de los componentes de SentinelStream, diferenciando las pruebas operativas de los resultados del experimento de Machine Learning.

## 2. Verificación del entorno

Ejecutar en PowerShell desde la raíz del proyecto:

```powershell

docker --version

docker compose version

docker compose ps

```

**Criterio:** Docker está disponible y los servicios aparecen con el estado esperado.

## 3. Verificación de Kafka

```powershell

docker compose logs --tail 100 kafka

```

**Criterio:** revisar errores de inicio y comprobar que los listeners coincidan con la configuración. El generador que se ejecuta en el host utiliza `localhost:9094`; los servicios dentro de Docker utilizan `kafka:9092`.

## 4. Verificación de Flink

```powershell

docker compose logs --tail 100 flink-jobmanager

docker compose logs --tail 100 flink-taskmanager

```

**Criterio:** revisar el inicio de los contenedores y los errores del procesamiento. Los logs, por sí solos, no demuestran que todas las pruebas funcionales hayan pasado.

## 5. PostgreSQL y Grafana

```powershell

docker compose logs --tail 100 postgres

docker compose logs --tail 100 grafana
```

**Criterio:** revisar los logs y verificar por separado la conexión a PostgreSQL y la carga de los dashboards en Grafana.

## 6. Validación de las capas de datos

- **Bronze:** comprobar la existencia y estructura de los archivos JSON generados.

- **Silver:** comprobar el esquema Parquet, los valores nulos, la unicidad de `call_id` y que `duration_sec` no contenga valores negativos.

- **Gold:** comprobar las métricas almacenadas en PostgreSQL y su visualización en Grafana.

No ejecutar el proceso Silver de producción únicamente para probarlo: puede sobrescribir `data/silver_data/silver_data.parquet`.

Estas comprobaciones deben realizarse sobre los datos existentes, utilizando consultas o inspecciones de solo lectura cuando corresponda. No deben ejecutarse procesos que sobrescriban archivos o tablas únicamente para obtener evidencia de validación.

## 7. Resultados históricos del experimento offline V9

Esta sección conserva los resultados históricos de V9. La versión más reciente del experimento de Machine Learning documentada en este repositorio es V11, cuyos resultados se presentan en el README principal.

El experimento utiliza datos sintéticos de CDR y una separación temporal entre entrenamiento, calibración y prueba. El modelo Isolation Forest se evaluó fuera del flujo de producción.

| Métrica | Resultado |

|---|---:|

| Precision | 0.8511 |

| Recall | 0.9091 |

| F1-score | 0.8791 |

| ROC-AUC | 0.9504 |

| Average Precision | 0.8562 |

Los resultados de V9 se conservan exclusivamente como referencia histórica y no deben compararse directamente con los resultados de V11 sin verificar que ambos experimentos utilicen las mismas condiciones de evaluación, características y conjuntos de datos.

Estos resultados corresponden al conjunto de prueba sintético. No demuestran detección de fraude real y el modelo no está integrado en el procesamiento de producción.

## 8. Pruebas automatizadas del consumer V11

El proyecto incluye pruebas automatizadas para verificar el comportamiento del consumer de inferencia V11.

Ejecutar desde PowerShell:

```powershell
.\.venv-inference\Scripts\python.exe -m pytest -q tests/test_consumer_v11.py
```
**Resultado registrado:** 10 pruebas automatizadas aprobadas para `tests/test_consumer_v11.py`.

**Alcance:** estas pruebas verifican el comportamiento del consumer V11. No representan una validación integral de Kafka, Flink, PostgreSQL, Grafana ni del pipeline completo.

## 9. Registro de resultados

Para cada comprobación, registrar:

- Fecha de ejecución.

- Componente verificado.

- Comando o procedimiento utilizado.

- Resultado observado.

- Errores encontrados y solución aplicada.

No marcar una prueba como aprobada si no se ha ejecutado y verificado.

## 10. Limitaciones

Los resultados operativos dependen de la configuración local, los datos disponibles y las versiones instaladas. Esta guía documenta los procedimientos de validación; no implica que todas las pruebas se hayan ejecutado satisfactoriamente.

La existencia de pruebas unitarias aprobadas no sustituye las pruebas de integración ni las pruebas de extremo a extremo del sistema.

## 11. Recuperación y sincronización del consumidor ML V11

**Fecha:** 04/11/2026

Se verificó la recuperación del consumidor `sentinelstream-v11-inference` después de acumular un backlog de 23,566 mensajes.

Comando de verificación:

```powershell
docker exec kafka-cdr /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group sentinelstream-v11-inference
```

Resultados de tres mediciones consecutivas:

| Offset actual | Log end offset | LAG |
|---:|---:|---:|
| 535029 | 535029 | 0 |
| 535103 | 535103 | 0 |
| 535155 | 535155 | 0 |

**Resultado:** el consumidor alcanzó el último offset disponible y mantuvo LAG 0 durante las tres verificaciones. Esto demuestra sincronización del grupo en la partición observada, no una garantía de procesamiento exactamente una vez.

## 12. Limitación operativa observada en ML V11

Durante las verificaciones del 04/11/2026 se observó una tasa de clasificación de anomalías del 100% en una ventana de 15 minutos:

- Predicciones registradas: 6,857.
- Clasificadas como anomalías: 6,857.
- Clasificadas como normales: 0.
- Tasa de anomalías: 100%.

En una consulta posterior de distribución de scores se obtuvieron estos valores:

| Indicador | Resultado |
|---|---:|
| Score mínimo | -0.01492 |
| Score mediano | 0.06674 |
| Score promedio | 0.06233 |
| Score máximo | 0.13804 |
| Umbral V11 | -0.02357 |
| Predicciones sobre el umbral | 5,824 de 5,824 |

Los totales corresponden a consultas ejecutadas en ventanas temporales distintas.

La comparación de características entre el conjunto sintético de entrenamiento y el tráfico generado durante la ejecución mostró diferencias relevantes:

| Característica | Entrenamiento | Tráfico generado |
|---|---:|---:|
| Llamadas previas 1 min (promedio) | 0.35 | 0.34 |
| Llamadas previas 5 min (promedio) | 0.82 | 1.32 |
| Llamadas previas 15 min (promedio) | 0.82 | 3.21 |
| Destinos únicos previos 24 h (promedio) | 1.36 | 16.15 |

**Conclusión:** el umbral calibrado con datos sintéticos no separa adecuadamente las observaciones del tráfico generado en ejecución. El 100% de clasificaciones como anomalía no representa una tasa real de fraude.

V11 permanece como integración experimental de inferencia en línea. Se requiere una futura recalibración y evaluación con datos representativos antes de considerarlo operativo para detección real de fraude.
