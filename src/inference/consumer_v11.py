import json
import logging
import os
import sqlite3
import sys
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import joblib
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata, TopicPartition

# SentinelStream V11: online Kafka inference consumer.
ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = ROOT / "experiments" / "models" / "isolation_forest_v11_baseline.joblib"
METADATA_PATH = ROOT / "experiments" / "models" / "isolation_forest_v11_baseline.json"
DB_PATH = Path(__file__).resolve().parent / "state_v11.sqlite3"

INPUT_TOPIC = "cdr_events"
OUTPUT_TOPIC = "cdr_ml_predictions_v11"
GROUP_ID = "sentinelstream-v11-inference"
BOOTSTRAP_SERVERS = ["localhost:9094"]

EXPECTED_FEATURES = [
    "prior_calls_1m",
    "prior_calls_5m",
    "prior_calls_15m",
    "prior_unique_destinations_24h",
]
WINDOWS_SECONDS = (60, 300, 900, 86400)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("sentinelstream.v11")


class InvalidEventError(ValueError):
    """Evento CDR con formato o campos no validos."""


def parse_timestamp(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp ausente o invalido")

    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        # Legacy timestamps without an offset are interpreted as UTC.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def timestamp_text(value):
    return value.isoformat(timespec="microseconds")


def initialize_database():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.executescript("""
        CREATE TABLE IF NOT EXISTS processed_events (
            call_id TEXT PRIMARY KEY,
            caller TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            status TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS caller_history (
            call_id TEXT PRIMARY KEY,
            caller TEXT NOT NULL,
            callee TEXT NOT NULL,
            event_ts TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS caller_state (
            caller TEXT PRIMARY KEY,
            latest_event_ts TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_history_caller_ts
        ON caller_history(caller, event_ts);

        CREATE TABLE IF NOT EXISTS prediction_outbox (
            call_id TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            published INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS processing_errors (
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            message_offset INTEGER NOT NULL,
            call_id TEXT,
            raw_message TEXT NOT NULL,
            error_type TEXT NOT NULL,
            error_message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (topic, partition_id, message_offset)
        );
    """)
    # Backfill idempotente: nunca reemplaza un estado ya existente.
    con.execute("""
        INSERT OR IGNORE INTO caller_state(caller, latest_event_ts)
        SELECT caller, MAX(event_ts)
        FROM caller_history
        GROUP BY caller
    """)

    # Safe cleanup: caller_state preserves the latest timestamp.
    for caller, latest_ts in con.execute(
        "SELECT caller, latest_event_ts FROM caller_state"
    ).fetchall():
        cutoff = timestamp_text(
            parse_timestamp(latest_ts) - timedelta(seconds=86400)
        )
        con.execute(
            "DELETE FROM caller_history WHERE caller = ? AND event_ts < ?",
            (caller, cutoff),
        )

    con.commit()
    return con


def get_features(con, caller, callee, event_time):
    current = event_time
    oldest = timestamp_text(current - timedelta(seconds=86400))
    current_text = timestamp_text(current)

    # Read only the caller history that can affect the 24-hour features.
    # Equal timestamps are resolved by Kafka arrival order: only rows
    # already committed to SQLite are visible when this function runs.
    history = con.execute(
        """
        SELECT event_ts, callee
        FROM caller_history
        WHERE caller = ? AND event_ts >= ? AND event_ts <= ?
        ORDER BY event_ts, call_id
        """,
        (caller, oldest, current_text),
    ).fetchall()

    prior_times = []
    unique_destinations = set()

    for event_ts, destination in history:
        prior_time = parse_timestamp(event_ts)
        age = (current - prior_time).total_seconds()

        if age < 0 or age > 86400:
            continue
        if age <= 86400:
            unique_destinations.add(destination)
        prior_times.append(age)

    return {
        "prior_calls_1m": sum(age <= 60 for age in prior_times),
        "prior_calls_5m": sum(age <= 300 for age in prior_times),
        "prior_calls_15m": sum(age <= 900 for age in prior_times),
        "prior_unique_destinations_24h": len(unique_destinations),
    }


def process_event(con, model, threshold, event):
    try:
        required = ("call_id", "caller", "callee", "timestamp", "duration_sec", "country_code")
        if not isinstance(event, dict) or any(
            key not in event or event[key] is None for key in required
        ):
            raise ValueError("evento CDR sin campos obligatorios")

        call_id = str(event["call_id"]).strip()
        caller = str(event["caller"]).strip()
        callee = str(event["callee"]).strip()
        if not call_id or not caller or not callee:
            raise ValueError("call_id, caller y callee no pueden estar vacios")

        event_time = parse_timestamp(event["timestamp"])
        event_ts = timestamp_text(event_time)

        # Validate fields without using the synthetic label as a model feature.
        duration = int(event["duration_sec"])
        if duration < 0:
            raise ValueError("duration_sec no puede ser negativo")
    except (ValueError, TypeError, OverflowError) as exc:
        raise InvalidEventError(str(exc)) from exc

    con.execute("BEGIN IMMEDIATE")
    try:
        existing = con.execute(
            "SELECT status FROM processed_events WHERE call_id = ?",
            (call_id,),
        ).fetchone()
        if existing:
            con.commit()
            return "duplicate"

        state = con.execute(
            "SELECT latest_event_ts FROM caller_state WHERE caller = ?",
            (caller,),
        ).fetchone()
        latest = state[0] if state else None

        if latest and event_ts < latest:
            con.execute(
                "INSERT INTO processed_events VALUES (?, ?, ?, ?)",
                (call_id, caller, event_ts, "late_event_skipped"),
            )
            con.commit()
            log.warning("Evento tardio omitido: call_id=%s caller=%s", call_id, caller)
            return "late_event_skipped"

        features = get_features(con, caller, callee, event_time)
        row = [[features[name] for name in EXPECTED_FEATURES]]

        score = float(-model.decision_function(row)[0])
        if not (-float("inf") < score < float("inf")):
            raise ValueError("score del modelo no finito")

        is_anomaly = score >= threshold
        prediction = {
            "call_id": call_id,
            "caller": caller,
            "callee": callee,
            "timestamp": event_ts,
            "model_name": "SentinelStream V11 baseline",
            "model_version": "V11",
            "anomaly_score": score,
            "threshold": threshold,
            "is_anomaly": bool(is_anomaly),
            "features": features,
            "status": "scored",
        }

        # State, deduplication record and outbox are committed atomically.
        con.execute(
            "INSERT INTO caller_history VALUES (?, ?, ?, ?)",
            (call_id, caller, callee, event_ts),
        )
        con.execute(
            "INSERT INTO processed_events VALUES (?, ?, ?, ?)",
            (call_id, caller, event_ts, "scored"),
        )
        con.execute(
            """
            INSERT INTO caller_state(caller, latest_event_ts)
            VALUES (?, ?)
            ON CONFLICT(caller) DO UPDATE SET
                latest_event_ts = MAX(
                    caller_state.latest_event_ts,
                    excluded.latest_event_ts
                )
            """,
            (caller, event_ts),
        )
        con.execute(
            """
            INSERT INTO prediction_outbox(call_id, payload, published, created_at)
            VALUES (?, ?, 0, ?)
            """,
            (call_id, json.dumps(prediction), timestamp_text(datetime.now(timezone.utc))),
        )

        # The boundary is inclusive: only events older than
        # a event_time - 24 horas.
        cutoff = timestamp_text(event_time - timedelta(seconds=86400))
        con.execute(
            "DELETE FROM caller_history WHERE caller = ? AND event_ts < ?",
            (caller, cutoff),
        )
        con.commit()
        return "scored"
    except Exception:
        con.rollback()
        raise


def record_invalid_message(con, message, raw_message, error):
    raw_text = (
        "<KAFKA_TOMBSTONE>"
        if raw_message is None
        else raw_message.decode("utf-8", errors="replace")
    )
    call_id = None

    try:
        candidate = json.loads(raw_text)
        if isinstance(candidate, dict) and candidate.get("call_id") is not None:
            call_id = str(candidate["call_id"])
    except (json.JSONDecodeError, TypeError):
        pass

    con.execute("BEGIN IMMEDIATE")
    try:
        con.execute(
            """
            INSERT OR IGNORE INTO processing_errors (
                topic, partition_id, message_offset, call_id,
                raw_message, error_type, error_message, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message.topic,
                message.partition,
                message.offset,
                call_id,
                raw_text,
                type(error).__name__,
                str(error),
                timestamp_text(datetime.now(timezone.utc)),
            ),
        )
        con.commit()
    except Exception:
        con.rollback()
        raise


def publish_pending(con, producer):
    rows = con.execute(
        """
        SELECT call_id, payload
        FROM prediction_outbox
        WHERE published = 0
        ORDER BY created_at, call_id
        """
    ).fetchall()

    for call_id, payload in rows:
        # Stable Kafka key helps downstream deduplication.
        producer.send(
            OUTPUT_TOPIC,
            key=call_id.encode("utf-8"),
            value=json.loads(payload),
        ).get(timeout=30)

        # A crash between send success and this update can cause a repeat.
        con.execute(
            "UPDATE prediction_outbox SET published = 1 WHERE call_id = ?",
            (call_id,),
        )
        con.commit()


def main():
    if not MODEL_PATH.is_file() or not METADATA_PATH.is_file():
        raise FileNotFoundError("No se encuentran el modelo y sus metadatos")

    with METADATA_PATH.open("r", encoding="utf-8-sig") as file:
        metadata = json.load(file)

    if metadata.get("features") != EXPECTED_FEATURES:
        raise ValueError("El orden de caracteristicas no coincide con el contrato V11")

    threshold = float(metadata["threshold"])
    model = joblib.load(MODEL_PATH)

    con = initialize_database()
    consumer = None
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            acks="all",
            retries=10,
            key_serializer=None,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

        # A new group starts at latest. Existing group offsets resume on restart.
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            key_deserializer=None,
            # Keep raw bytes so malformed JSON can be recorded in SQLite.
            value_deserializer=None,
            max_poll_records=1,
        )

        log.info("Consumidor V11 iniciado; entrada=%s salida=%s", INPUT_TOPIC, OUTPUT_TOPIC)

        while True:
            publish_pending(con, producer)
            records = consumer.poll(timeout_ms=1000, max_records=1)

            for _, messages in records.items():
                for message in messages:
                    raw_message = message.value

                    if raw_message is None:
                        exc = InvalidEventError(
                            "mensaje tombstone de Kafka (value=None)"
                        )
                        record_invalid_message(con, message, raw_message, exc)
                        result = "invalid_message"
                    else:
                        try:
                            event = json.loads(raw_message.decode("utf-8"))
                        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                            record_invalid_message(con, message, raw_message, exc)
                            result = "invalid_json"
                        else:
                            try:
                                result = process_event(con, model, threshold, event)
                            except InvalidEventError as exc:
                                record_invalid_message(con, message, raw_message, exc)
                                result = "invalid_event"

                    # Confirm only this message, after its SQLite transaction.
                    consumer.commit({
                        TopicPartition(message.topic, message.partition):
                            OffsetAndMetadata(message.offset + 1, "")
                    })

                    log.info(
                        "Resultado=%s topic=%s partition=%s offset=%s",
                        result, message.topic, message.partition, message.offset
                    )
    finally:
        if consumer is not None:
            consumer.close()
        if producer is not None:
            producer.flush(timeout=10)
            producer.close()
        con.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Interrupcion solicitada")
        sys.exit(0)
