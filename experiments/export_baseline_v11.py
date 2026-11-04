from pathlib import Path
from datetime import datetime, timezone
import json

import numpy as np
import polars as pl
import joblib
import sklearn
from sklearn.ensemble import IsolationForest
from sklearn.metrics import f1_score, precision_score

SEED = 42
EXPECTED_THRESHOLD = -0.02356832
THRESHOLD_TOLERANCE = 1e-7

DATA_PATH = Path("experiments/synthetic_data/cdr_experiment_v10.parquet")
REPORT_PATH = Path("experiments/results/anomaly_evaluation_v11.txt")
PREDICTIONS_PATH = Path("experiments/results/anomaly_predictions_v11.parquet")
MODEL_PATH = Path("experiments/models/isolation_forest_v11_baseline.joblib")
METADATA_PATH = Path("experiments/models/isolation_forest_v11_baseline.json")

FEATURES = [
    "prior_calls_1m",
    "prior_calls_5m",
    "prior_calls_15m",
    "prior_unique_destinations_24h",
]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def choose_threshold(y_true, scores):
    best = None
    for threshold in np.unique(scores):
        predictions = (scores >= threshold).astype(int)
        f1 = f1_score(y_true, predictions, zero_division=0)
        precision = precision_score(
            y_true, predictions, zero_division=0
        )
        candidate = (f1, precision, float(threshold))
        if best is None or candidate[:2] > best[:2]:
            best = candidate
    return best[2], best[0], best[1]


def main():
    # Preflight: preservar resultados V11 y no sobrescribir exportaciones.
    require(DATA_PATH.is_file(), f"No existe el dataset: {DATA_PATH}")
    require(REPORT_PATH.is_file(), f"No existe el informe V11: {REPORT_PATH}")
    require(
        PREDICTIONS_PATH.is_file(),
        f"No existen las predicciones V11: {PREDICTIONS_PATH}",
    )
    require(
        not MODEL_PATH.exists() and not METADATA_PATH.exists(),
        "Ya existe al menos un destino de exportación. No se sobrescribirá.",
    )

    report = REPORT_PATH.read_text(encoding="utf-8")
    require(
        "Umbral seleccionado en calibración: -0.02356832" in report,
        "El umbral esperado no coincide con el informe actual. Abortando.",
    )

    df = pl.read_parquet(DATA_PATH).sort(["caller", "timestamp", "call_id"])
    required = ["caller", "is_burst_event", *FEATURES]
    missing = [c for c in required if c not in df.columns]
    require(not missing, f"Faltan columnas: {missing}")
    require(df.height == 10000, f"Filas inesperadas: {df.height}")
    require(df["caller"].n_unique() == 500, "No hay 500 llamantes.")
    require(
        all(df[c].null_count() == 0 for c in required),
        "Hay nulos en las columnas de entrenamiento.",
    )

    # Reproducir exactamente la partición por llamante de V11.
    callers = sorted(df["caller"].unique().to_list())
    shuffled = np.array(callers, dtype=object)
    np.random.default_rng(SEED).shuffle(shuffled)

    train_callers = set(shuffled[:300].tolist())
    calibration_callers = set(shuffled[300:400].tolist())

    train_df = df.filter(pl.col("caller").is_in(list(train_callers)))
    calibration_df = df.filter(
        pl.col("caller").is_in(list(calibration_callers))
    )

    X_train = train_df.select(FEATURES).to_numpy()
    X_calibration = calibration_df.select(FEATURES).to_numpy()
    y_calibration = calibration_df["is_burst_event"].cast(
        pl.Int8
    ).to_numpy()

    require(len(X_train) > 0 and len(X_calibration) > 0,
            "Particiones vacías.")
    require(y_calibration.sum() > 0, "Calibración sin positivos.")

    model = IsolationForest(
        n_estimators=300,
        contamination=0.05,
        random_state=SEED,
        n_jobs=-1,
    )
    model.fit(X_train)

    scores = -model.decision_function(X_calibration)
    threshold, calibration_f1, calibration_precision = choose_threshold(
        y_calibration, scores
    )

    print(f"Umbral recalculado: {threshold:.8f}")
    print(f"Umbral esperado:    {EXPECTED_THRESHOLD:.8f}")

    require(
        abs(threshold - EXPECTED_THRESHOLD) <= THRESHOLD_TOLERANCE,
        "El umbral no coincide. No se guardará ningún artefacto.",
    )

    # Solo crear y escribir las salidas después de validar todo.
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

    metadata = {
        "model_name": "SentinelStream V11 baseline",
        "model_type": "sklearn.ensemble.IsolationForest",
        "features": FEATURES,
        "threshold": float(threshold),
        "score_definition": "-model.decision_function(X); mayor indica mayor anomalía",
        "threshold_selection": "Máximo F1 en calibración; desempate por precisión",
        "calibration_f1": float(calibration_f1),
        "calibration_precision": float(calibration_precision),
        "n_estimators": 300,
        "contamination": 0.05,
        "random_state": SEED,
        "train_callers": 300,
        "calibration_callers": 100,
        "dataset": str(DATA_PATH),
        "dataset_type": "sintético",
        "scikit_learn_version": sklearn.__version__,
        "joblib_version": joblib.__version__,
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "limitations": [
            "Las etiquetas y los datos son sintéticos.",
            "El score no es una probabilidad calibrada.",
            "Este artefacto no implementa por sí solo la inferencia en streaming.",
        ],
    }

    # Modo exclusivo: no sobrescribir archivos preexistentes.
    with MODEL_PATH.open("xb") as handle:
        joblib.dump(model, handle)

    try:
        with METADATA_PATH.open("x", encoding="utf-8") as handle:
            json.dump(metadata, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    except Exception:
        # No borrar automáticamente el modelo si falla el segundo archivo.
        print("ATENCIÓN: el modelo se guardó, pero falló el JSON.")
        print(f"Modelo: {MODEL_PATH}")
        raise

    # Verificación de carga del modelo exportado.
    loaded = joblib.load(MODEL_PATH)
    check_scores = -loaded.decision_function(X_calibration[:5])
    require(np.isfinite(check_scores).all(), "Score no finito al recargar.")

    print("\nEXPORTACIÓN COMPLETADA")
    print(f"Modelo:    {MODEL_PATH}")
    print(f"Metadatos: {METADATA_PATH}")
    print(f"Características: {FEATURES}")
    print("Resultados V11 originales: preservados.")
    print("Modelo exportado: recargado y verificado.")


if __name__ == "__main__":
    main()
