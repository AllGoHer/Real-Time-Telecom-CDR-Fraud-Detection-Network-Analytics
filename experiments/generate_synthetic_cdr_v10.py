from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random

import numpy as np
import polars as pl

SEED = 20261032
N_CALLS = 10_000
N_CALLERS = 500
CALLS_PER_CALLER = N_CALLS // N_CALLERS
DAYS = 30
N_EPISODES = 100
MIN_EPISODE_CALLS = 10
MAX_EPISODE_CALLS = 16
MAX_EPISODE_SECONDS = 240

OUTPUT_FILE = Path("experiments/synthetic_data/cdr_experiment_v10.parquet")

if N_CALLS % N_CALLERS != 0:
    raise ValueError("N_CALLS debe ser divisible entre N_CALLERS.")
if N_EPISODES > N_CALLERS:
    raise ValueError("No puede haber más episodios que abonados.")
if OUTPUT_FILE.exists():
    raise FileExistsError(f"No se sobrescribirá: {OUTPUT_FILE}")

rng = random.Random(SEED)
np_rng = np.random.default_rng(SEED)

COUNTRIES = ["ES", "US", "GB", "DE", "FR", "NG", "BR"]
PREFIXES = {
    "ES": "34", "US": "1", "GB": "44", "DE": "49",
    "FR": "33", "NG": "234", "BR": "55",
}

callers = [f"CALLER-{i:04d}" for i in range(N_CALLERS)]
episode_callers = set(rng.sample(callers, N_EPISODES))
start = datetime(2026, 1, 1, tzinfo=timezone.utc)

records = []
call_number = 0
episode_number = 0

for caller_index, caller in enumerate(callers):
    country = rng.choice(COUNTRIES)
    prefix = PREFIXES[country]

    habitual_destinations = [
        f"{prefix}{700000000 + rng.randint(0, 99999999):09d}"
        for _ in range(rng.randint(4, 7))
    ]

    events = []
    for _ in range(CALLS_PER_CALLER):
        offset = rng.randint(0, DAYS * 86400 - 1)
        callee = (
            rng.choice(habitual_destinations)
            if rng.random() < 0.92
            else f"{prefix}{800000000 + rng.randint(0, 99999999):09d}"
        )
        events.append({
            "offset": offset,
            "callee": callee,
            "burst_id": None,
            "burst_call_index": None,
        })

    if caller in episode_callers:
        burst_id = f"BURST-V10-{episode_number:04d}"
        episode_day = round(
            episode_number * (DAYS - 1) / max(1, N_EPISODES - 1)
        )
        episode_start = episode_day * 86400 + rng.randint(0, 20 * 3600)
        episode_size = rng.randint(
            MIN_EPISODE_CALLS, MAX_EPISODE_CALLS
        )
        selected_indices = rng.sample(
            range(CALLS_PER_CALLER), episode_size
        )

        burst_events = []
        for event_index in selected_indices:
            callee = (
                f"{prefix}{800000000 + rng.randint(0, 99999999):09d}"
                if rng.random() < 0.90
                else rng.choice(habitual_destinations)
            )
            burst_events.append({
                "event_index": event_index,
                "offset": episode_start + rng.randint(
                    0, MAX_EPISODE_SECONDS
                ),
                "callee": callee,
            })

        burst_events.sort(
            key=lambda item: (item["offset"], item["event_index"])
        )

        for index, item in enumerate(burst_events, start=1):
            events[item["event_index"]] = {
                "offset": item["offset"],
                "callee": item["callee"],
                "burst_id": burst_id,
                "burst_call_index": index,
            }

        episode_number += 1

    for event in events:
        duration = max(
            1, min(int(np_rng.lognormal(mean=4.2, sigma=1.0)), 1800)
        )
        records.append({
            "call_id": f"EXP10-{call_number:07d}",
            "caller": caller,
            "callee": event["callee"],
            "duration_sec": duration,
            "country_code": country,
            "timestamp": (
                start + timedelta(seconds=event["offset"])
            ).isoformat(),
            "burst_id": event["burst_id"],
            "burst_call_index": event["burst_call_index"],
            "is_burst_event": event["burst_id"] is not None,
        })
        call_number += 1

df = pl.DataFrame(
    records,
    schema_overrides={
        "burst_id": pl.String,
        "burst_call_index": pl.Int64,
    },
).sort(["caller", "timestamp", "call_id"])

# Features causales: cada fila solo utiliza llamadas anteriores
# del mismo abonado. Las ventanas son inclusivas en su límite.
feature_rows = []
for caller_df in df.partition_by("caller", maintain_order=True):
    times = [
        datetime.fromisoformat(value)
        for value in caller_df["timestamp"].to_list()
    ]
    destinations = caller_df["callee"].to_list()

    history_1m = deque()
    history_5m = deque()
    history_15m = deque()
    history_24h = deque()
    destination_counts = {}

    for i, current_time in enumerate(times):
        now = current_time.timestamp()

        while history_1m and now - history_1m[0][0] > 60:
            history_1m.popleft()
        while history_5m and now - history_5m[0][0] > 300:
            history_5m.popleft()
        while history_15m and now - history_15m[0][0] > 900:
            history_15m.popleft()
        while history_24h and now - history_24h[0][0] > 86400:
            old_time, old_destination = history_24h.popleft()
            destination_counts[old_destination] -= 1
            if destination_counts[old_destination] == 0:
                del destination_counts[old_destination]

        feature_rows.append({
            "call_id": caller_df["call_id"][i],
            "prior_calls_1m": len(history_1m),
            "prior_calls_5m": len(history_5m),
            "prior_calls_15m": len(history_15m),
            "prior_unique_destinations_24h": len(destination_counts),
        })

        # Añadir la llamada actual solo después de calcular sus features.
        item = (now, destinations[i])
        history_1m.append(item)
        history_5m.append(item)
        history_15m.append(item)
        history_24h.append(item)
        destination_counts[destinations[i]] = (
            destination_counts.get(destinations[i], 0) + 1
        )

features = pl.DataFrame(feature_rows)
df = df.join(features, on="call_id", how="left").sort(
    ["timestamp", "call_id"]
)

required = [
    "call_id", "caller", "callee", "duration_sec", "country_code",
    "timestamp", "is_burst_event", "prior_calls_1m",
    "prior_calls_5m", "prior_calls_15m",
    "prior_unique_destinations_24h",
]
if df.height != N_CALLS:
    raise ValueError(f"Número inesperado de registros: {df.height}")
if df["call_id"].n_unique() != N_CALLS:
    raise ValueError("Hay IDs de llamadas duplicados.")
if df.select([
    pl.col(column).is_null().sum().alias(column)
    for column in required
]).row(0) != tuple(0 for _ in required):
    raise ValueError("Hay nulos en columnas requeridas.")
if not df["timestamp"].is_sorted():
    raise ValueError("El dataset no está ordenado cronológicamente.")

burst_rows = df.filter(pl.col("is_burst_event"))
if burst_rows["burst_id"].n_unique() != N_EPISODES:
    raise ValueError("La cantidad de episodios no coincide.")

episode_checks = burst_rows.group_by("burst_id").agg(
    pl.len().alias("n_calls"),
    pl.col("caller").n_unique().alias("n_callers"),
    pl.col("burst_call_index").n_unique().alias("n_indices"),
    pl.col("burst_call_index").min().alias("min_index"),
    pl.col("burst_call_index").max().alias("max_index"),
)

if episode_checks.filter(
    (pl.col("n_calls") < MIN_EPISODE_CALLS)
    | (pl.col("n_calls") > MAX_EPISODE_CALLS)
    | (pl.col("n_callers") != 1)
    | (pl.col("n_indices") != pl.col("n_calls"))
    | (pl.col("min_index") != 1)
    | (pl.col("max_index") != pl.col("n_calls"))
).height:
    raise ValueError("Hay episodios con cantidad de llamadas o índices inválidos.")

if df.filter(
    (pl.col("prior_calls_1m") < 0)
    | (pl.col("prior_calls_5m") < pl.col("prior_calls_1m"))
    | (pl.col("prior_calls_15m") < pl.col("prior_calls_5m"))
    | (pl.col("prior_unique_destinations_24h") < 0)
).height:
    raise ValueError("Hay características temporales inválidas.")
if df.filter(
    (~pl.col("is_burst_event")) & pl.col("burst_id").is_not_null()
).height:
    raise ValueError("Hay llamadas normales con burst_id.")

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
if OUTPUT_FILE.exists():
    raise FileExistsError(f"No se sobrescribirá: {OUTPUT_FILE}")

df.write_parquet(OUTPUT_FILE)

print("GENERADOR SINTÉTICO V10")
print(f"Archivo: {OUTPUT_FILE}")
print(f"Llamadas: {df.height}")
print(f"Abonados: {df['caller'].n_unique()}")
print(f"Episodios: {burst_rows['burst_id'].n_unique()}")
print(f"Llamadas etiquetadas: {burst_rows.height}")
print(f"Orden cronológico: {df['timestamp'].is_sorted()}")
