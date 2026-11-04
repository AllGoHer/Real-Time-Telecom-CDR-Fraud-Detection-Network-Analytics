import json
import random
import time
from collections import deque
from datetime import datetime, timezone

from kafka import KafkaProducer

# SentinelStream: generador de CDR con abonados recurrentes y episodios
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9094"]
TOPIC_NAME = "cdr_events"

CALLS_PER_SECOND = 2
N_CALLERS = 500

COUNTRIES = ["ES", "US", "GB", "DE", "FR", "NG", "BR"]

PREFIXES = {
    "ES": "34",
    "US": "1",
    "GB": "44",
    "DE": "49",
    "FR": "33",
    "NG": "234",
    "BR": "55",
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

# Cada abonado conserva su país y sus destinos habituales.
callers = {}

for i in range(N_CALLERS):
    country = random.choice(COUNTRIES)
    prefix = PREFIXES[country]

    callers[f"CALLER-{i:04d}"] = {
        "country_code": country,
        "prefix": prefix,
        "habitual_destinations": [
            f"{prefix}{random.randint(700000000, 799999999)}"
            for _ in range(random.randint(4, 7))
        ],
    }

caller_ids = list(callers.keys())

call_counter = 0
episode_counter = 0
active_burst = deque()

next_episode_at = time.monotonic() + 30


def generate_normal_destination(caller):
    profile = callers[caller]

    if random.random() < 0.92:
        return random.choice(profile["habitual_destinations"])

    return f"{profile['prefix']}{random.randint(800000000, 899999999)}"


def generate_burst_destination(caller):
    profile = callers[caller]

    # El 90 % de las llamadas del episodio utiliza destinos nuevos.
    if random.random() < 0.90:
        return f"{profile['prefix']}{random.randint(800000000, 899999999)}"

    return random.choice(profile["habitual_destinations"])


def schedule_burst():
    global episode_counter

    caller = random.choice(caller_ids)
    episode_counter += 1

    burst_id = f"BURST-STREAM-{episode_counter:05d}"
    episode_size = random.randint(10, 16)

    now = time.monotonic()
    elapsed = 0
    events = []

    for index in range(1, episode_size + 1):
        if index > 1:
            elapsed += random.randint(8, 16)

        events.append({
            "due": now + elapsed,
            "caller": caller,
            "callee": generate_burst_destination(caller),
            "burst_id": burst_id,
            "burst_call_index": index,
        })

    for event in events:
        active_burst.append(event)

    print(
        f"[EPISODIO] {burst_id} | "
        f"Abonado: {caller} | "
        f"Llamadas: {episode_size} | "
        f"Duración: {elapsed} segundos"
    )

    return now + elapsed + 90


def generate_cdr():
    global call_counter

    call_counter += 1

    is_burst = False
    burst_id = None
    burst_call_index = None

    # Si hay una llamada de episodio pendiente, se emite cuando corresponde.
    if active_burst and active_burst[0]["due"] <= time.monotonic():
        event = active_burst.popleft()

        caller = event["caller"]
        callee = event["callee"]
        burst_id = event["burst_id"]
        burst_call_index = event["burst_call_index"]
        is_burst = True

    else:
        caller = random.choice(caller_ids)
        callee = generate_normal_destination(caller)

    profile = callers[caller]

    # Distribución de duración independiente del patrón de anomalía.
    if random.random() < 0.10:
        duration = random.randint(1, 5)
    else:
        duration = max(
            1,
            min(int(random.lognormvariate(4.2, 1.0)), 1800)
        )

    now = datetime.now(timezone.utc)

    return {
        "call_id": f"CDR-{int(time.time() * 1000)}-{call_counter}",
        "caller": caller,
        "callee": callee,
        "duration_sec": duration,
        "country_code": profile["country_code"],
        "timestamp": now.isoformat(),
        "is_suspicious": duration <= 5,
        "is_burst_event": is_burst,
        "burst_id": burst_id,
        "burst_call_index": burst_call_index,
    }


if __name__ == "__main__":
    print("=" * 60)
    print("SENTINELSTREAM - CDR GENERATOR")
    print(f"Abonados: {N_CALLERS}")
    print(f"Frecuencia objetivo: {CALLS_PER_SECOND} llamadas/segundo")
    print("Episodios: 10-16 llamadas en un máximo de 240 segundos")
    print("Kafka: localhost:9094")
    print("=" * 60)

    try:
        while True:
            now = time.monotonic()

            if not active_burst and now >= next_episode_at:
                next_episode_at = schedule_burst()

            cdr = generate_cdr()

            producer.send(TOPIC_NAME, value=cdr)

            label = "EPISODIO" if cdr["is_burst_event"] else "NORMAL"

            print(
                f"[{label}] "
                f"{cdr['call_id']} | "
                f"{cdr['caller']} -> {cdr['callee']} | "
                f"Duración: {cdr['duration_sec']}s | "
                f"País: {cdr['country_code']}"
            )

            producer.flush()
            time.sleep(1 / CALLS_PER_SECOND)

    except KeyboardInterrupt:
        print("\nGenerador detenido por el usuario.")

    finally:
        producer.flush()
        producer.close()
