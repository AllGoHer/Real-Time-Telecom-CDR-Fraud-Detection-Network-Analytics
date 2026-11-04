import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import joblib
import pytest

# Allow importing the existing V11 consumer without modifying it.
ROOT = Path(__file__).resolve().parents[1]
INFERENCE_DIR = ROOT / "src" / "inference"
sys.path.insert(0, str(INFERENCE_DIR))

import consumer_v11 as v11


THRESHOLD = -0.02356831650735658
BASE_TIME = datetime(2026, 10, 30, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(tmp_path, monkeypatch):
    """Use a temporary SQLite database; never touch production state."""
    test_db_path = tmp_path / "test_state.sqlite3"
    monkeypatch.setattr(v11, "DB_PATH", test_db_path)

    con = v11.initialize_database()
    yield con
    con.close()


@pytest.fixture
def model():
    """Load the actual V11 model from the repository."""
    return joblib.load(v11.MODEL_PATH)


def make_event(call_id, caller, callee, event_time):
    return {
        "call_id": call_id,
        "caller": caller,
        "callee": callee,
        "timestamp": event_time.isoformat(),
        "duration_sec": 60,
        "country_code": "ES",
    }


def test_repeated_calls_update_features_and_score(db, model):
    caller = "CALLER-TEST-001"

    first = make_event("TEST-001", caller, "DEST-A", BASE_TIME)
    second = make_event(
        "TEST-002", caller, "DEST-B", BASE_TIME + timedelta(seconds=30)
    )
    third = make_event(
        "TEST-003", caller, "DEST-C", BASE_TIME + timedelta(seconds=60)
    )

    assert v11.process_event(db, model, THRESHOLD, first) == "scored"
    assert v11.process_event(db, model, THRESHOLD, second) == "scored"
    assert v11.process_event(db, model, THRESHOLD, third) == "scored"

    row = db.execute(
        "SELECT payload FROM prediction_outbox WHERE call_id = ?",
        ("TEST-003",),
    ).fetchone()

    prediction = json.loads(row[0])

    assert prediction["features"] == {
        "prior_calls_1m": 2,
        "prior_calls_5m": 2,
        "prior_calls_15m": 2,
        "prior_unique_destinations_24h": 2,
    }
    assert prediction["anomaly_score"] == pytest.approx(-0.00038002, abs=1e-7)
    assert prediction["is_anomaly"] is True


def test_duplicate_event_is_not_processed_twice(db, model):
    event = make_event(
        "DUPLICATE-001", "CALLER-DUP", "DEST-A", BASE_TIME
    )

    assert v11.process_event(db, model, THRESHOLD, event) == "scored"
    assert v11.process_event(db, model, THRESHOLD, event) == "duplicate"

    assert db.execute(
        "SELECT COUNT(*) FROM processed_events WHERE call_id = ?",
        ("DUPLICATE-001",),
    ).fetchone()[0] == 1

    assert db.execute(
        "SELECT COUNT(*) FROM prediction_outbox WHERE call_id = ?",
        ("DUPLICATE-001",),
    ).fetchone()[0] == 1


def test_late_event_is_skipped(db, model):
    caller = "CALLER-LATE"

    newer = make_event(
        "LATE-NEWER", caller, "DEST-A", BASE_TIME + timedelta(minutes=1)
    )
    older = make_event(
        "LATE-OLDER", caller, "DEST-B", BASE_TIME
    )

    assert v11.process_event(db, model, THRESHOLD, newer) == "scored"
    assert v11.process_event(db, model, THRESHOLD, older) == "late_event_skipped"

    status = db.execute(
        "SELECT status FROM processed_events WHERE call_id = ?",
        ("LATE-OLDER",),
    ).fetchone()[0]

    assert status == "late_event_skipped"
    assert db.execute(
        "SELECT COUNT(*) FROM caller_history WHERE caller = ?",
        (caller,),
    ).fetchone()[0] == 1


def test_temporal_windows_are_inclusive(db):
    caller = "CALLER-BOUNDARIES"
    current = BASE_TIME

    ages = [60, 61, 300, 301, 900, 901, 86400, 86401]

    for index, age in enumerate(ages):
        event_time = current - timedelta(seconds=age)
        db.execute(
            """
            INSERT INTO caller_history(call_id, caller, callee, event_ts)
            VALUES (?, ?, ?, ?)
            """,
            (
                f"BOUNDARY-{index}",
                caller,
                f"DEST-{index}",
                v11.timestamp_text(event_time),
            ),
        )

    db.commit()

    features = v11.get_features(
        db, caller, "DEST-CURRENT", current
    )

    assert features == {
        "prior_calls_1m": 1,
        "prior_calls_5m": 3,
        "prior_calls_15m": 5,
        "prior_unique_destinations_24h": 7,
    }


def test_database_backfill_is_persistent_and_idempotent(tmp_path, monkeypatch):
    test_db_path = tmp_path / "backfill_test.sqlite3"
    monkeypatch.setattr(v11, "DB_PATH", test_db_path)

    # Simulate an existing database with history but without caller_state.
    legacy = sqlite3.connect(test_db_path)
    legacy.execute(
        """
        CREATE TABLE caller_history (
            call_id TEXT PRIMARY KEY,
            caller TEXT NOT NULL,
            callee TEXT NOT NULL,
            event_ts TEXT NOT NULL
        )
        """
    )

    legacy.execute(
        "INSERT INTO caller_history VALUES (?, ?, ?, ?)",
        ("OLD-001", "CALLER-A", "DEST-A", "2026-10-30T10:00:00.000000+00:00"),
    )
    legacy.execute(
        "INSERT INTO caller_history VALUES (?, ?, ?, ?)",
        ("OLD-002", "CALLER-A", "DEST-B", "2026-10-30T11:00:00.000000+00:00"),
    )
    legacy.execute(
        "INSERT INTO caller_history VALUES (?, ?, ?, ?)",
        ("OLD-003", "CALLER-B", "DEST-C", "2026-10-30T10:30:00.000000+00:00"),
    )
    legacy.commit()
    legacy.close()

    first = v11.initialize_database()

    states_first = first.execute(
        "SELECT caller, latest_event_ts FROM caller_state ORDER BY caller"
    ).fetchall()

    assert len(states_first) == 2
    assert states_first[0][0] == "CALLER-A"
    assert states_first[0][1] == "2026-10-30T11:00:00.000000+00:00"
    first.close()

    second = v11.initialize_database()

    assert second.execute(
        "SELECT COUNT(*) FROM caller_state"
    ).fetchone()[0] == 2

    assert second.execute(
        "SELECT COUNT(*) FROM caller_history"
    ).fetchone()[0] == 3

    second.close()


def test_inference_failure_rolls_back_all_changes(db):
    class FailingModel:
        def decision_function(self, features):
            raise RuntimeError("simulated model failure")

    event = make_event(
        "ROLLBACK-001", "CALLER-ROLLBACK", "DEST-A", BASE_TIME
    )

    with pytest.raises(RuntimeError, match="simulated model failure"):
        v11.process_event(db, FailingModel(), THRESHOLD, event)

    for table in (
        "processed_events",
        "caller_history",
        "caller_state",
        "prediction_outbox",
    ):
        count = db.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]
        assert count == 0, f"Unexpected partial data in {table}"

    assert not db.in_transaction


def test_invalid_event_is_rejected_without_database_changes(db, model):
    invalid_event = {
        "call_id": "INVALID-001",
        "caller": "CALLER-INVALID",
        # Missing required fields.
    }

    with pytest.raises(v11.InvalidEventError):
        v11.process_event(db, model, THRESHOLD, invalid_event)

    assert db.execute(
        "SELECT COUNT(*) FROM processed_events"
    ).fetchone()[0] == 0

    assert db.execute(
        "SELECT COUNT(*) FROM prediction_outbox"
    ).fetchone()[0] == 0

def test_publish_pending_uses_call_id_as_kafka_key_and_marks_published(db):
    class FakeFuture:
        def get(self, timeout):
            assert timeout == 30
            return True

    class FakeProducer:
        def __init__(self):
            self.messages = []

        def send(self, topic, key, value):
            self.messages.append({
                "topic": topic,
                "key": key,
                "value": value,
            })
            return FakeFuture()

    call_id = "OUTBOX-001"
    payload = {
        "call_id": call_id,
        "status": "scored",
        "is_anomaly": False,
    }

    db.execute(
        """
        INSERT INTO prediction_outbox(call_id, payload, published, created_at)
        VALUES (?, ?, 0, ?)
        """,
        (
            call_id,
            json.dumps(payload),
            v11.timestamp_text(BASE_TIME),
        ),
    )
    db.commit()

    producer = FakeProducer()

    v11.publish_pending(db, producer)

    assert len(producer.messages) == 1
    assert producer.messages[0]["topic"] == v11.OUTPUT_TOPIC
    assert producer.messages[0]["key"] == call_id.encode("utf-8")
    assert producer.messages[0]["value"] == payload

    published = db.execute(
        "SELECT published FROM prediction_outbox WHERE call_id = ?",
        (call_id,),
    ).fetchone()[0]

    assert published == 1

    # A second execution must not publish the same pending record again.
    v11.publish_pending(db, producer)
    assert len(producer.messages) == 1


def test_record_invalid_message_is_deduplicated_by_kafka_offset(db):
    from types import SimpleNamespace

    message = SimpleNamespace(
        topic="cdr_events",
        partition=0,
        offset=12345,
    )
    raw = b'{"call_id":"INVALID-JSON"}'

    v11.record_invalid_message(
        db, message, raw, ValueError("malformed JSON")
    )
    v11.record_invalid_message(
        db, message, raw, ValueError("malformed JSON")
    )

    row = db.execute(
        """
        SELECT call_id, raw_message, error_type, error_message
        FROM processing_errors
        WHERE topic = ? AND partition_id = ? AND message_offset = ?
        """,
        ("cdr_events", 0, 12345),
    ).fetchone()

    assert row == (
        "INVALID-JSON",
        '{"call_id":"INVALID-JSON"}',
        "ValueError",
        "malformed JSON",
    )
    assert db.execute(
        "SELECT COUNT(*) FROM processing_errors"
    ).fetchone()[0] == 1



def test_record_kafka_tombstone(db):
    from types import SimpleNamespace

    message = SimpleNamespace(
        topic="cdr_events",
        partition=0,
        offset=12346,
    )

    v11.record_invalid_message(
        db,
        message,
        None,
        ValueError("Kafka tombstone"),
    )

    row = db.execute(
        """
        SELECT call_id, raw_message, error_type, error_message
        FROM processing_errors
        WHERE topic = ? AND partition_id = ? AND message_offset = ?
        """,
        ("cdr_events", 0, 12346),
    ).fetchone()

    assert row == (
        None,
        "<KAFKA_TOMBSTONE>",
        "ValueError",
        "Kafka tombstone",
    )
