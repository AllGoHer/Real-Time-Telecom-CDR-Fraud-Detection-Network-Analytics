# SentinelStream - Pruebas automatizadas V11

## Objetivo

Documentar las pruebas automatizadas del consumidor de inferencia V11, basado en Isolation Forest.

## Requisitos

- Python 3.12.
- Dependencias del consumidor V11 instaladas.
- Modelo V11 disponible en `experiments/models/`.
- Pytest instalado en el entorno virtual.

Instalar pytest:

    .\src\inference\venv\Scripts\python.exe -m pip install pytest

## Ejecutar las pruebas

Desde la raíz del repositorio:

    .\src\inference\venv\Scripts\python.exe -m pytest .\tests\test_consumer_v11.py -v

## Cobertura

| N.º | Prueba | Validación |
|---:|---|---|
| 1 | Repeated calls | Actualización de características y puntuación ante llamadas repetidas. |
| 2 | Duplicate event | Evita procesar dos veces el mismo call_id. |
| 3 | Late event | Omite eventos atrasados. |
| 4 | Temporal windows | Comprueba los límites inclusivos de las ventanas temporales. |
| 5 | Database backfill | Verifica la reconstrucción persistente e idempotente del estado. |
| 6 | Inference rollback | Comprueba la reversión ante fallos de inferencia. |
| 7 | Invalid event | Verifica el rechazo de eventos inválidos. |
| 8 | Kafka outbox | Comprueba publicación, clave call_id y estado publicado. |
| 9 | Invalid message deduplication | Evita duplicados para el mismo offset Kafka. |
| 10 | Kafka tombstone | Comprueba el registro de mensajes tombstone. |

## Aislamiento

Las pruebas utilizan bases SQLite temporales y un productor Kafka simulado cuando corresponde.

No modifican:
- La base SQLite de producción.
- Los topics ni los consumidores Kafka reales.
- Los jobs Flink.
- Los datos de PostgreSQL.

## Alcance y limitaciones

Estas pruebas validan la lógica del consumidor mediante escenarios controlados. No sustituyen una prueba end-to-end con tráfico real.

La detección de anomalías ante llamadas repetidas de un mismo usuario todavía requiere validación con tráfico representativo.

## Última validación

- Pruebas ejecutadas: 10.
- Pruebas aprobadas: 10.
- Pruebas fallidas: 0.
- Resultado: `10 passed`.
