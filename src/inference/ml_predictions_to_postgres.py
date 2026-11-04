
import json
import logging
import os
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition


# ---------------------------------------------------------
# CONFIGURACION
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

KAFKA_BOOTSTRAP = "localhost:9094"
INPUT_TOPIC = "cdr_ml_predictions_v11"
GROUP_ID = "sentinelstream-ml-postgres-sink"

DB_CONFIG = {
    "host": "localhost",
    "port": "5433",
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "connect_timeout": 10,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("sentinelstream-ml-sink")


# ---------------------------------------------------------
# TABLA ML: NO MODIFICA TABLAS EXISTENTES
# ---------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.fraud_ml_predictions (
    id BIGSERIAL PRIMARY KEY,
    call_id VARCHAR(100) NOT NULL UNIQUE,
    caller VARCHAR(30) NOT NULL,
    callee VARCHAR(30) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    model_name VARCHAR(150) NOT NULL,
    model_version VARCHAR(30) NOT NULL,
    anomaly_score DOUBLE PRECISION NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    features JSONB NOT NULL,
    status VARCHAR(30) NOT NULL,
    raw_payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fraud_ml_predictions_detected
ON public.fraud_ml_predictions (is_anomaly, event_timestamp);

CREATE INDEX IF NOT EXISTS idx_fraud_ml_predictions_caller
ON public.fraud_ml_predictions (caller, event_timestamp);
"""

INSERT_SQL = """
INSERT INTO public.fraud_ml_predictions (
    call_id,
    caller,
    callee,
    event_timestamp,
    model_name,
    model_version,
    anomaly_score,
    threshold,
    is_anomaly,
    features,
    status,
    raw_payload
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (call_id) DO NOTHING;
"""


def validate_config():
    required = ("dbname", "user", "password")
    missing = [key for key in required if not DB_CONFIG.get(key)]

    if missing:
        raise RuntimeError(
            "Faltan variables PostgreSQL en .env: "
            + ", ".join(missing)
        )


def connect_database():
    connection = psycopg2.connect(**DB_CONFIG)

    with connection.cursor() as cursor:
        cursor.execute(CREATE_TABLE_SQL)

    connection.commit()

    logger.info(
        "PostgreSQL conectado. Tabla fraud_ml_predictions disponible."
    )

    return connection


def persist_prediction(connection, payload):
    required = (
        "call_id",
        "caller",
        "callee",
        "timestamp",
        "model_name",
        "model_version",
        "anomaly_score",
        "threshold",
        "is_anomaly",
        "features",
        "status",
    )

    missing = [field for field in required if field not in payload]

    if missing:
        raise ValueError(
            "Prediccion incompleta. Campos ausentes: "
            + ", ".join(missing)
        )

    event_timestamp = datetime.fromisoformat(
        payload["timestamp"].replace("Z", "+00:00")
    )

    values = (
        payload["call_id"],
        payload["caller"],
        payload["callee"],
        event_timestamp,
        payload["model_name"],
        payload["model_version"],
        float(payload["anomaly_score"]),
        float(payload["threshold"]),
        bool(payload["is_anomaly"]),
        Json(payload["features"]),
        payload["status"],
        Json(payload),
    )

    with connection.cursor() as cursor:
        cursor.execute(INSERT_SQL, values)
        inserted = cursor.rowcount == 1

    connection.commit()

    return inserted


def main():
    validate_config()

    connection = None
    consumer = None

    inserted_count = 0
    duplicate_count = 0

    try:
        connection = connect_database()

        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=1,
        )

        logger.info("Consumidor iniciado correctamente.")
        logger.info("Kafka topic: %s", INPUT_TOPIC)
        logger.info("Consumer group: %s", GROUP_ID)
        logger.info("Esperando predicciones ML...")

        # Lectura continua: no termina cuando no hay mensajes disponibles.
        while True:

            records = consumer.poll(
                timeout_ms=1000,
                max_records=1,
            )

            if not records:
                continue

            for topic_partition, messages in records.items():

                for message in messages:

                    try:
                        payload = json.loads(
                            message.value.decode("utf-8")
                        )

                        # Primero guardar y confirmar PostgreSQL.
                        inserted = persist_prediction(
                            connection,
                            payload,
                        )

                        # Después confirmar exactamente este offset.
                        next_offset = OffsetAndMetadata(
                            message.offset + 1,
                            "",
                        )

                        consumer.commit({
                            TopicPartition(
                                message.topic,
                                message.partition,
                            ): next_offset
                        })

                        if inserted:
                            inserted_count += 1

                            logger.info(
                                "ML GUARDADO | call_id=%s | anomaly=%s | score=%.6f",
                                payload["call_id"],
                                payload["is_anomaly"],
                                float(payload["anomaly_score"]),
                            )

                        else:
                            duplicate_count += 1

                            logger.info(
                                "DUPLICADO OMITIDO | call_id=%s",
                                payload["call_id"],
                            )

                        if (
                            inserted_count + duplicate_count
                        ) % 100 == 0:

                            logger.info(
                                "ACUMULADO | nuevos=%s | duplicados=%s",
                                inserted_count,
                                duplicate_count,
                            )

                    except Exception:
                        connection.rollback()

                        logger.exception(
                            "ERROR procesando mensaje | topic=%s | partition=%s | offset=%s. Offset sin confirmar.",
                            message.topic,
                            message.partition,
                            message.offset,
                        )

                        raise

    except KeyboardInterrupt:
        logger.info("Cierre solicitado por el usuario.")

    finally:

        if consumer is not None:
            consumer.close()

        if connection is not None:
            connection.close()

        logger.info(
            "Consumidor cerrado | nuevos=%s | duplicados=%s",
            inserted_count,
            duplicate_count,
        )


if __name__ == "__main__":
    main()
