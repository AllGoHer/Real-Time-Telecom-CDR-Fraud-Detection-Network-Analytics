from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random

import polars as pl

# ============================================================
# SENTINELSTREAM V12 - DATASET REPRESENTATIVO DE PRODUCCION
# ============================================================

SEED = 20261103
N_CALLERS = 500
DAYS = 3
CALLS_PER_SECOND = 2
N_CALLS = DAYS * 86400 * CALLS_PER_SECOND

MIN_EPISODE_CALLS = 10
MAX_EPISODE_CALLS = 16
EPISODE_COOLDOWN_SECONDS = 90

OUTPUT_FILE = Path(
    "experiments/synthetic_data/cdr_experiment_v12.parquet"
)

if OUTPUT_FILE.exists():
    raise FileExistsError(f"No se sobrescribira: {OUTPUT_FILE}")

rng = random.Random(SEED)

COUNTRIES = ["ES", "US", "GB", "DE", "FR", "NG", "BR"]

PREFIXES = {
    "ES": "34", "US": "1", "GB": "44",
    "DE": "49", "FR": "33", "NG": "234", "BR": "55",
}

START = datetime(2026, 1, 1, tzinfo=timezone.utc)

callers = [f"CALLER-{i:04d}" for i in range(N_CALLERS)]

profiles = {}

for caller in callers:
    country = rng.choice(COUNTRIES)
    prefix = PREFIXES[country]

    profiles[caller] = {
        "country_code": country,
        "prefix": prefix,
        "habitual_destinations": [
            f"{prefix}{700000000 + rng.randint(0, 99999999):09d}"
            for _ in range(rng.randint(4, 7))
        ],
    }


def new_destination(caller):
    prefix = profiles[caller]["prefix"]
    return f"{prefix}{800000000 + rng.randint(0, 99999999):09d}"


def normal_destination(caller):
    profile = profiles[caller]

    if rng.random() < 0.92:
        return rng.choice(profile["habitual_destinations"])

    return new_destination(caller)


def burst_destination(caller):
    profile = profiles[caller]

    if rng.random() < 0.90:
        return new_destination(caller)

    return rng.choice(profile["habitual_destinations"])


# ============================================================
# SIMULACION DEL FLUJO GLOBAL Y EPISODIOS INTERCALADOS
# ============================================================

records = []
active_burst = deque()

episode_counter = 0
call_counter = 0

next_episode_at = 30.0


def schedule_burst(now):
    global episode_counter

    caller = rng.choice(callers)
    episode_counter += 1

    burst_id = f"BURST-V12-{episode_counter:05d}"
    episode_size = rng.randint(
        MIN_EPISODE_CALLS, MAX_EPISODE_CALLS
    )

    elapsed = 0

    for index in range(1, episode_size + 1):
        if index > 1:
            elapsed += rng.randint(8, 18)

        active_burst.append({
            "due": now + elapsed,
            "caller": caller,
            "callee": burst_destination(caller),
            "burst_id": burst_id,
            "burst_call_index": index,
        })

    return now + elapsed + EPISODE_COOLDOWN_SECONDS


for slot in range(N_CALLS):

    now = slot / CALLS_PER_SECOND

    if not active_burst and now >= next_episode_at and now + 270 < DAYS * 86400:
        next_episode_at = schedule_burst(now)

    is_burst = False
    burst_id = None
    burst_call_index = None

    if active_burst and active_burst[0]["due"] <= now:
        event = active_burst.popleft()

        caller = event["caller"]
        callee = event["callee"]
        burst_id = event["burst_id"]
        burst_call_index = event["burst_call_index"]
        is_burst = True

    else:
        caller = rng.choice(callers)
        callee = normal_destination(caller)

    profile = profiles[caller]

    if rng.random() < 0.10:
        duration = rng.randint(1, 5)
    else:
        duration = max(
            1,
            min(int(rng.lognormvariate(4.2, 1.0)), 1800)
        )

    timestamp = (
        START + timedelta(milliseconds=slot * 500)
    ).isoformat(timespec="microseconds")

    call_counter += 1

    records.append({
        "call_id": f"EXP12-{call_counter:08d}",
        "caller": caller,
        "callee": callee,
        "duration_sec": duration,
        "country_code": profile["country_code"],
        "timestamp": timestamp,
        "burst_id": burst_id,
        "burst_call_index": burst_call_index,
        "is_burst_event": is_burst,
    })


print("Simulacion de llamadas terminada.")
print(f"Llamadas generadas: {len(records)}")
print(f"Episodios generados: {episode_counter}")


# ============================================================
# FEATURES CAUSALES: MISMA LOGICA DEL CONSUMIDOR V11
# ============================================================

df = pl.DataFrame(
    records,
    schema_overrides={
        "burst_id": pl.String,
        "burst_call_index": pl.Int64,
    },
).sort(["caller", "timestamp", "call_id"])

feature_rows = []

for caller_df in df.partition_by("caller", maintain_order=True):

    times = [
        datetime.fromisoformat(value).timestamp()
        for value in caller_df["timestamp"].to_list()
    ]

    destinations = caller_df["callee"].to_list()

    history_1m = deque()
    history_5m = deque()
    history_15m = deque()
    history_24h = deque()

    destination_counts = {}

    for i, current_time in enumerate(times):

        while history_1m and current_time - history_1m[0][0] > 60:
            history_1m.popleft()

        while history_5m and current_time - history_5m[0][0] > 300:
            history_5m.popleft()

        while history_15m and current_time - history_15m[0][0] > 900:
            history_15m.popleft()

        while history_24h and current_time - history_24h[0][0] > 86400:
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

        destination = destinations[i]
        item = (current_time, destination)

        history_1m.append(item)
        history_5m.append(item)
        history_15m.append(item)
        history_24h.append(item)

        destination_counts[destination] = (
            destination_counts.get(destination, 0) + 1
        )


features = pl.DataFrame(feature_rows)

df = df.join(features, on="call_id", how="left").sort(
    ["timestamp", "call_id"]
)


# ============================================================
# VALIDACIONES
# ============================================================

required = [
    "call_id", "caller", "callee", "duration_sec",
    "country_code", "timestamp", "is_burst_event",
    "prior_calls_1m", "prior_calls_5m",
    "prior_calls_15m", "prior_unique_destinations_24h",
]

if df.height != N_CALLS:
    raise ValueError("Cantidad de llamadas incorrecta.")

if df["call_id"].n_unique() != N_CALLS:
    raise ValueError("Hay call_id duplicados.")

if df.select([
    pl.col(column).is_null().sum().alias(column)
    for column in required
]).row(0) != tuple(0 for _ in required):
    raise ValueError("Hay valores nulos en columnas requeridas.")

if not df["timestamp"].is_sorted():
    raise ValueError("El dataset no esta ordenado cronologicamente.")

burst_rows = df.filter(pl.col("is_burst_event"))

if burst_rows["burst_id"].n_unique() != episode_counter:
    raise ValueError("Cantidad de episodios incorrecta.")

episode_checks = burst_rows.group_by("burst_id").agg(
    pl.len().alias("n_calls"),
    pl.col("caller").n_unique().alias("n_callers"),
    pl.col("burst_call_index").n_unique().alias("n_indices"),
    pl.col("burst_call_index").min().alias("min_index"),
    pl.col("burst_call_index").max().alias("max_index"),
)

invalid_episodes = episode_checks.filter(
    (pl.col("n_calls") < MIN_EPISODE_CALLS)
    | (pl.col("n_calls") > MAX_EPISODE_CALLS)
    | (pl.col("n_callers") != 1)
    | (pl.col("n_indices") != pl.col("n_calls"))
    | (pl.col("min_index") != 1)
    | (pl.col("max_index") != pl.col("n_calls"))
)

if invalid_episodes.height:
    print("\n========== EPISODIOS INVALIDOS ==========")
    print(f"Cantidad: {invalid_episodes.height}")
    print(invalid_episodes.head(20))
    raise ValueError("Hay episodios invalidos. Revisar tabla anterior.")

if df.filter(
    (pl.col("prior_calls_1m") < 0)
    | (pl.col("prior_calls_5m") < pl.col("prior_calls_1m"))
    | (pl.col("prior_calls_15m") < pl.col("prior_calls_5m"))
    | (pl.col("prior_unique_destinations_24h") < 0)
).height:
    raise ValueError("Hay features temporales invalidas.")

if df.filter(
    (~pl.col("is_burst_event")) & pl.col("burst_id").is_not_null()
).height:
    raise ValueError("Llamadas normales con burst_id.")

if df.filter(
    pl.col("is_burst_event") != pl.col("burst_id").is_not_null()
).height:
    raise ValueError("Etiquetas y burst_id inconsistentes.")


# ============================================================
# EXPORTACION SIN SOBRESCRIBIR
# ============================================================

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

if OUTPUT_FILE.exists():
    raise FileExistsError(f"No se sobrescribira: {OUTPUT_FILE}")

df.write_parquet(OUTPUT_FILE)

print("\n========== SENTINELSTREAM V12 ==========")
print(f"Archivo: {OUTPUT_FILE}")
print(f"Llamadas: {df.height}")
print(f"Abonados: {df['caller'].n_unique()}")
print(f"Episodios: {episode_counter}")
print(f"Llamadas de episodio: {burst_rows.height}")
print(f"Llamadas normales: {df.height - burst_rows.height}")
print(f"Orden cronologico: {df['timestamp'].is_sorted()}")

print("\n========== DISTRIBUCION DE FEATURES ==========")

print(
    df.select([
        pl.col("prior_calls_1m").median().alias("mediana_1m"),
        pl.col("prior_calls_5m").median().alias("mediana_5m"),
        pl.col("prior_calls_15m").median().alias("mediana_15m"),
        pl.col("prior_unique_destinations_24h").median().alias(
            "mediana_destinos_24h"
        ),
        pl.col("prior_unique_destinations_24h").quantile(0.95).alias(
            "p95_destinos_24h"
        ),
    ])
)

print("\nV12 generado y validado.")
