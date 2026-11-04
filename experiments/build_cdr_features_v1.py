from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

INPUT_FILE = Path("experiments/synthetic_data/cdr_experiment_v2.parquet")
OUTPUT_FILE = Path("experiments/synthetic_data/cdr_features_v1.parquet")

if not INPUT_FILE.exists():
    raise FileNotFoundError(f"No existe el archivo de entrada: {INPUT_FILE}")

if OUTPUT_FILE.exists():
    raise FileExistsError(
        f"No se sobrescribirá el archivo existente: {OUTPUT_FILE}"
    )

df = pl.read_parquet(INPUT_FILE)

required = {
    "call_id", "caller", "callee", "duration_sec",
    "timestamp", "is_suspicious"
}
missing = required - set(df.columns)
if missing:
    raise ValueError(f"Faltan columnas obligatorias: {sorted(missing)}")

if df["call_id"].n_unique() != df.height:
    raise ValueError("call_id contiene duplicados")

# Interpretar las fechas ISO en Python, sin depender de tzdata.
records = df.to_dicts()
for row in records:
    row["_dt"] = datetime.fromisoformat(
        row["timestamp"].replace("Z", "+00:00")
    )

# Procesar cada abonado en orden cronológico.
# Las ventanas solo consideran la llamada actual y llamadas anteriores.
feature_rows = []

for caller in df["caller"].unique().to_list():
    caller_rows = [
        row for row in records if row["caller"] == caller
    ]
    caller_rows.sort(key=lambda row: (row["_dt"], row["call_id"]))

    history = []
    for row in caller_rows:
        current_time = row["_dt"]

        # Retener llamadas dentro de las últimas 24 horas,
        # incluida la llamada actual.
        history = [
            previous for previous in history
            if current_time - previous["_dt"] <= timedelta(hours=24)
        ]

        # calls_5m: llamadas del abonado en los últimos 5 minutos,
        # incluida la actual.
        calls_5m = sum(
            current_time - previous["_dt"] <= timedelta(minutes=5)
            for previous in history
        ) + 1

        # Destinos distintos en las últimas 24 horas,
        # incluida la llamada actual.
        unique_destinations_24h = len(
            {previous["callee"] for previous in history}
            | {row["callee"]}
        )

        feature_rows.append({
            "call_id": row["call_id"],
            "calls_5m": calls_5m,
            "unique_destinations_24h": unique_destinations_24h,
        })

        history.append(row)

features = pl.DataFrame(feature_rows)

# Unir por identificador único; mantener la etiqueta solo como objetivo.
result = df.select([
    "call_id", "caller", "duration_sec", "timestamp", "is_suspicious"
]).join(features, on="call_id", how="left")

# Orden de columnas pensado para el siguiente paso de modelado.
result = result.select([
    "call_id",
    "caller",
    "timestamp",
    "duration_sec",
    "calls_5m",
    "unique_destinations_24h",
    "is_suspicious",
])

if result.height != df.height:
    raise RuntimeError("El número de filas cambió inesperadamente")

if result["call_id"].n_unique() != df.height:
    raise RuntimeError("La salida no conserva IDs de llamada únicos")

if any(result.null_count().row(0)):
    raise RuntimeError("Se encontraron valores nulos en la salida")

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
result.write_parquet(OUTPUT_FILE)

print(f"Archivo creado: {OUTPUT_FILE}")
print(f"Filas: {result.height}")
print(f"Columnas: {result.width}")
print(f"Abonados únicos: {result['caller'].n_unique()}")
print("\nVariables disponibles:")
print(result.columns)
print("\nResumen de variables:")
print(
    result.select([
        "duration_sec",
        "calls_5m",
        "unique_destinations_24h",
    ]).describe()
)
print("\nEtiquetas:")
print(result.group_by("is_suspicious").len().sort("is_suspicious"))
