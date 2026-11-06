import csv
import io
import json
import subprocess
from pathlib import Path

import joblib
import numpy as np
import polars as pl

MODEL_PATH = Path("experiments/models/isolation_forest_v12_baseline.joblib")
METADATA_PATH = Path("experiments/models/isolation_forest_v12_baseline.json")
DATASET_PATH = Path("experiments/synthetic_data/cdr_experiment_v12.parquet")

FEATURES = [
    "prior_calls_1m",
    "prior_calls_5m",
    "prior_calls_15m",
    "prior_unique_destinations_24h",
]

SQL = """
COPY (
    SELECT
        features->>'prior_calls_1m' AS prior_calls_1m,
        features->>'prior_calls_5m' AS prior_calls_5m,
        features->>'prior_calls_15m' AS prior_calls_15m,
        features->>'prior_unique_destinations_24h' AS prior_unique_destinations_24h,
        anomaly_score,
        CASE WHEN is_anomaly THEN 1 ELSE 0 END AS v11_prediction
    FROM public.fraud_ml_predictions
    WHERE status = 'scored'
      AND caller LIKE 'CALLER-%'
      AND received_at >= NOW() - INTERVAL '24 hours'
      AND features ? 'prior_calls_1m'
      AND features ? 'prior_calls_5m'
      AND features ? 'prior_calls_15m'
      AND features ? 'prior_unique_destinations_24h'
) TO STDOUT WITH CSV HEADER
"""


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def describe(values):
    return {
        "p50": float(np.quantile(values, 0.50)),
        "p95": float(np.quantile(values, 0.95)),
        "p99": float(np.quantile(values, 0.99)),
        "max": float(np.max(values)),
    }


def main():
    for path in [MODEL_PATH, METADATA_PATH, DATASET_PATH]:
        require(path.is_file(), f"No existe: {path}")

    print("Consultando PostgreSQL sin modificar datos...")

    command = [
        "docker", "exec",
        "postgres-fraud",
        "psql",
        "-X", "-q", "-A",
        "-U", "fraud_user",
        "-d", "fraud_detection",
        "-c", SQL,
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )

    require(
        result.returncode == 0,
        f"Error consultando PostgreSQL:\n{result.stderr}",
    )

    rows = list(csv.DictReader(io.StringIO(result.stdout)))

    require(len(rows) > 0, "PostgreSQL no devolvió registros")

    print(f"Registros recuperados: {len(rows):,}")

    # Preparar las cuatro características en el orden del modelo.
    X = np.array(
        [
            [float(row[column]) for column in FEATURES]
            for row in rows
        ],
        dtype=np.float64,
    )

    v11_scores = np.array(
        [float(row["anomaly_score"]) for row in rows],
        dtype=np.float64,
    )

    v11_predictions = np.array(
        [int(row["v11_prediction"]) for row in rows],
        dtype=np.int8,
    )

    require(np.isfinite(X).all(), "Hay features no finitas")
    require(np.isfinite(v11_scores).all(), "Hay scores V11 no finitos")

    # Cargar V12 y verificar el esquema.
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))

    require(metadata["features"] == FEATURES, "Features incompatibles")
    threshold = float(metadata["threshold"])

    model = joblib.load(MODEL_PATH)

    print("Calculando scores V12 sobre producción...")

    v12_scores = -model.decision_function(X)
    v12_predictions = (v12_scores >= threshold).astype(np.int8)

    require(np.isfinite(v12_scores).all(), "Scores V12 no finitos")

    # Comparación de decisiones.
    agreement = float(np.mean(v11_predictions == v12_predictions))

    v11_alerts = int(v11_predictions.sum())
    v12_alerts = int(v12_predictions.sum())

    v11_rate = 100.0 * v11_alerts / len(rows)
    v12_rate = 100.0 * v12_alerts / len(rows)

    v11_only = int(np.sum((v11_predictions == 1) & (v12_predictions == 0)))
    v12_only = int(np.sum((v11_predictions == 0) & (v12_predictions == 1)))
    both = int(np.sum((v11_predictions == 1) & (v12_predictions == 1)))
    neither = int(np.sum((v11_predictions == 0) & (v12_predictions == 0)))

    # Distribuciones de features: producción frente a V12 sintético.
    synthetic = pl.read_parquet(DATASET_PATH)

    print("\n" + "=" * 65)
    print("SENTINELSTREAM - EVALUACION V12 SOBRE PRODUCCION")
    print("=" * 65)

    print(f"\nRegistros reales evaluados: {len(rows):,}")
    print(f"Umbral V12: {threshold:.8f}")

    print("\nTASA DE ALERTAS")
    print(f"V11: {v11_alerts:,} ({v11_rate:.2f}%)")
    print(f"V12: {v12_alerts:,} ({v12_rate:.2f}%)")
    print(f"Diferencia V12 - V11: {v12_rate - v11_rate:+.2f} puntos porcentuales")

    print("\nCOMPARACION DE DECISIONES")
    print(f"Acuerdo entre modelos: {agreement * 100:.2f}%")
    print(f"Alertas de ambos modelos: {both:,}")
    print(f"Solo V11: {v11_only:,}")
    print(f"Solo V12: {v12_only:,}")
    print(f"Ninguno alerta: {neither:,}")

    print("\nDISTRIBUCION DE SCORES")
    print("V11:", describe(v11_scores))
    print("V12:", describe(v12_scores))

    print("\nDISTRIBUCION DE FEATURES")
    for feature in FEATURES:
        real_values = X[:, FEATURES.index(feature)]
        synthetic_values = synthetic[feature].to_numpy()

        real_stats = describe(real_values)
        synthetic_stats = describe(synthetic_values)

        print(f"\n{feature}")
        print(
            f"  Produccion: p50={real_stats['p50']:.2f}, "
            f"p95={real_stats['p95']:.2f}, "
            f"p99={real_stats['p99']:.2f}"
        )
        print(
            f"  Sintetico:  p50={synthetic_stats['p50']:.2f}, "
            f"p95={synthetic_stats['p95']:.2f}, "
            f"p99={synthetic_stats['p99']:.2f}"
        )

    print("\nLIMITACIONES")
    print("- No existen etiquetas verificadas de fraude en esta muestra.")
    print("- La comparacion mide comportamiento, no precision ni recall.")
    print("- No se modifico PostgreSQL ni el consumidor V11.")
    print("- No se escribieron predicciones en las tablas de produccion.")

    print("\nDIAGNOSTICO COMPLETADO")


if __name__ == "__main__":
    main()
