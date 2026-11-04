from pathlib import Path
from datetime import datetime, timezone
import json

import joblib
import numpy as np
import polars as pl
import sklearn

from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

SEED = 42
N_ESTIMATORS = 300
CONTAMINATION = 0.05

DATA_PATH = Path("experiments/synthetic_data/cdr_experiment_v12.parquet")
REPORT_PATH = Path("experiments/results/anomaly_evaluation_v12.txt")
PREDICTIONS_PATH = Path("experiments/results/anomaly_predictions_v12.parquet")
MODEL_PATH = Path("experiments/models/isolation_forest_v12_baseline.joblib")
METADATA_PATH = Path("experiments/models/isolation_forest_v12_baseline.json")

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

    # --------------------------------------------------------
    # 1. PREFLIGHT: NO SOBRESCRIBIR NINGUN ARTEFACTO
    # --------------------------------------------------------

    outputs = [
        REPORT_PATH,
        PREDICTIONS_PATH,
        MODEL_PATH,
        METADATA_PATH,
    ]

    existing = [str(path) for path in outputs if path.exists()]

    require(not existing, f"Ya existen destinos V12: {existing}")
    require(DATA_PATH.is_file(), f"No existe el dataset: {DATA_PATH}")

    # --------------------------------------------------------
    # 2. CARGA Y VALIDACION
    # --------------------------------------------------------

    print("Cargando dataset V12...")

    df = pl.read_parquet(DATA_PATH)

    required = [
        "call_id",
        "caller",
        "timestamp",
        "is_burst_event",
        *FEATURES,
    ]

    missing = [column for column in required if column not in df.columns]

    require(not missing, f"Faltan columnas: {missing}")
    require(df.height == 518400, f"Filas inesperadas: {df.height}")
    require(df["call_id"].n_unique() == df.height, "call_id duplicados")
    require(df["caller"].n_unique() == 500, "Cantidad de abonados incorrecta")

    require(
        all(df[column].null_count() == 0 for column in required),
        "Hay valores nulos en columnas obligatorias",
    )

    require(
        df["is_burst_event"].sum() > 0,
        "No existen llamadas positivas",
    )

    require(
        df.filter(
            (pl.col("prior_calls_1m") < 0)
            | (pl.col("prior_calls_5m") < pl.col("prior_calls_1m"))
            | (pl.col("prior_calls_15m") < pl.col("prior_calls_5m"))
            | (pl.col("prior_unique_destinations_24h") < 0)
        ).height == 0,
        "Hay features temporales invalidas",
    )

    df = df.sort(["caller", "timestamp", "call_id"])

    # --------------------------------------------------------
    # 3. PARTICION POR ABONADO: 300 / 100 / 100
    # --------------------------------------------------------

    callers = sorted(df["caller"].unique().to_list())

    shuffled = np.array(callers, dtype=object)
    np.random.default_rng(SEED).shuffle(shuffled)

    train_callers = set(shuffled[:300].tolist())
    calibration_callers = set(shuffled[300:400].tolist())
    test_callers = set(shuffled[400:].tolist())

    require(
        not (train_callers & calibration_callers)
        and not (train_callers & test_callers)
        and not (calibration_callers & test_callers),
        "Hay abonados compartidos entre particiones",
    )

    train_df = df.filter(
        pl.col("caller").is_in(list(train_callers))
    )

    calibration_df = df.filter(
        pl.col("caller").is_in(list(calibration_callers))
    )

    test_df = df.filter(
        pl.col("caller").is_in(list(test_callers))
    )

    require(
        train_df.height + calibration_df.height + test_df.height == df.height,
        "La particion perdio o duplico llamadas",
    )

    y_calibration = calibration_df["is_burst_event"].cast(
        pl.Int8
    ).to_numpy()

    y_test = test_df["is_burst_event"].cast(pl.Int8).to_numpy()

    require(
        len(np.unique(y_calibration)) == 2,
        "Calibracion debe contener positivos y negativos",
    )

    require(
        len(np.unique(y_test)) == 2,
        "Prueba debe contener positivos y negativos",
    )

    print("\nPARTICION VALIDADA")
    print(f"Entrenamiento: {train_df.height:,} llamadas / 300 abonados")
    print(f"Calibracion:   {calibration_df.height:,} llamadas / 100 abonados")
    print(f"Prueba:        {test_df.height:,} llamadas / 100 abonados")

    # --------------------------------------------------------
    # 4. ENTRENAMIENTO BASELINE V12
    # --------------------------------------------------------

    print("\nEntrenando Isolation Forest V12...")

    X_train = train_df.select(FEATURES).to_numpy()
    X_calibration = calibration_df.select(FEATURES).to_numpy()
    X_test = test_df.select(FEATURES).to_numpy()

    model = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,
        random_state=SEED,
        n_jobs=-1,
    )

    model.fit(X_train)

    # Mayor score = mayor anomalía.
    calibration_scores = -model.decision_function(X_calibration)

    threshold, calibration_f1, calibration_precision = choose_threshold(
        y_calibration,
        calibration_scores,
    )

    print(f"Umbral V12: {threshold:.8f}")
    print(f"F1 calibracion: {calibration_f1:.4f}")
    print(f"Precision calibracion: {calibration_precision:.4f}")

    # --------------------------------------------------------
    # 5. EVALUACION EN ABONADOS NO UTILIZADOS
    # --------------------------------------------------------

    test_scores = -model.decision_function(X_test)
    y_prediction = (test_scores >= threshold).astype(int)

    tn, fp, fn, tp = confusion_matrix(
        y_test,
        y_prediction,
        labels=[0, 1],
    ).ravel()

    metrics = {
        "accuracy": float(accuracy_score(y_test, y_prediction)),
        "precision": float(precision_score(
            y_test, y_prediction, zero_division=0
        )),
        "recall": float(recall_score(
            y_test, y_prediction, zero_division=0
        )),
        "f1": float(f1_score(
            y_test, y_prediction, zero_division=0
        )),
        "pr_auc": float(average_precision_score(y_test, test_scores)),
        "roc_auc": float(roc_auc_score(y_test, test_scores)),
    }

    positives = int(y_test.sum())
    predicted_positives = int(y_prediction.sum())

    # --------------------------------------------------------
    # 6. INFORME
    # --------------------------------------------------------

    lines = [
        "SENTINELSTREAM - EXPERIMENTO V12",
        "================================",
        f"Dataset: {DATA_PATH}",
        f"Llamadas: {df.height}",
        f"Abonados: {len(callers)}",
        f"Episodios: {df.filter(pl.col('is_burst_event'))['burst_id'].n_unique()}",
        f"Features: {', '.join(FEATURES)}",
        "",
        "PARTICIONES",
        f"Entrenamiento: {train_df.height} llamadas / 300 abonados",
        f"Calibracion: {calibration_df.height} llamadas / 100 abonados",
        f"Prueba: {test_df.height} llamadas / 100 abonados",
        "",
        "MODELO",
        "IsolationForest",
        f"Estimadores: {N_ESTIMATORS}",
        f"Contamination: {CONTAMINATION}",
        f"Random state: {SEED}",
        f"Umbral seleccionado en calibracion: {threshold:.8f}",
        f"F1 calibracion: {calibration_f1:.4f}",
        f"Precision calibracion: {calibration_precision:.4f}",
        "",
        "RESULTADOS EN PRUEBA",
        f"Llamadas de prueba: {len(y_test)}",
        f"Positivos reales: {positives}",
        f"Predicciones positivas: {predicted_positives}",
        f"TN={int(tn)} FP={int(fp)} FN={int(fn)} TP={int(tp)}",
        f"Accuracy: {metrics['accuracy']:.4f}",
        f"Precision: {metrics['precision']:.4f}",
        f"Recall: {metrics['recall']:.4f}",
        f"F1: {metrics['f1']:.4f}",
        f"PR-AUC: {metrics['pr_auc']:.4f}",
        f"ROC-AUC: {metrics['roc_auc']:.4f}",
        "",
        "LIMITACIONES",
        "- Dataset y etiquetas sinteticos.",
        "- Los resultados no demuestran rendimiento con fraude real.",
        "- El score no es una probabilidad calibrada.",
        "- La evaluacion se realiza sobre abonados no utilizados en entrenamiento.",
    ]

    report = "\n".join(lines) + "\n"

    # --------------------------------------------------------
    # 7. PREDICCIONES DE PRUEBA
    # --------------------------------------------------------

    predictions = test_df.select([
        "call_id",
        "caller",
        "timestamp",
        "is_burst_event",
        "burst_id",
        "burst_call_index",
    ]).with_columns([
        pl.Series("v12_score", test_scores),
        pl.Series("v12_prediction", y_prediction),
        pl.lit(float(threshold)).alias("v12_threshold"),
    ])

    require(
        predictions.height == test_df.height,
        "Cantidad incorrecta de predicciones",
    )

    # --------------------------------------------------------
    # 8. METADATOS DEL MODELO
    # --------------------------------------------------------

    metadata = {
        "model_name": "SentinelStream V12 baseline",
        "model_version": "V12",
        "model_type": "sklearn.ensemble.IsolationForest",
        "features": FEATURES,
        "threshold": float(threshold),
        "score_definition": "-model.decision_function(X); mayor indica mayor anomalia",
        "threshold_selection": "Maximo F1 en calibracion; desempate por precision",
        "calibration_f1": float(calibration_f1),
        "calibration_precision": float(calibration_precision),
        "test_metrics": metrics,
        "confusion_matrix": {
            "tn": int(tn),
            "fp": int(fp),
            "fn": int(fn),
            "tp": int(tp),
        },
        "n_estimators": N_ESTIMATORS,
        "contamination": CONTAMINATION,
        "random_state": SEED,
        "train_callers": 300,
        "calibration_callers": 100,
        "test_callers": 100,
        "dataset": str(DATA_PATH),
        "dataset_type": "sintetico",
        "scikit_learn_version": sklearn.__version__,
        "joblib_version": joblib.__version__,
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "limitations": [
            "Las etiquetas y los datos son sinteticos.",
            "El score no es una probabilidad calibrada.",
            "El modelo requiere validacion con trafico real etiquetado.",
            "Este artefacto no implementa por si solo la inferencia en streaming.",
        ],
    }

    # --------------------------------------------------------
    # 9. EXPORTACION EXCLUSIVA
    # --------------------------------------------------------

    require(
        not any(path.exists() for path in outputs),
        "Un destino V12 aparecio durante la ejecucion. Exportacion cancelada.",
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

    with MODEL_PATH.open("xb") as handle:
        joblib.dump(model, handle)

    try:
        with METADATA_PATH.open("x", encoding="utf-8") as handle:
            json.dump(metadata, handle, ensure_ascii=False, indent=2)
            handle.write("\n")

        with PREDICTIONS_PATH.open("xb") as handle:
            predictions.write_parquet(handle)

        with REPORT_PATH.open("x", encoding="utf-8") as handle:
            handle.write(report)

    except Exception:
        print("ERROR durante la exportacion.")
        print("Se conservan los archivos que hayan llegado a escribirse.")
        raise

    # Verificar que el modelo guardado puede volver a cargarse.
    loaded = joblib.load(MODEL_PATH)
    check = -loaded.decision_function(X_test[:10])

    require(np.isfinite(check).all(), "Modelo exportado con scores no finitos")

    print("\n" + report)
    print("========== EXPORTACION V12 COMPLETADA ==========")
    print(f"Modelo: {MODEL_PATH}")
    print(f"Metadatos: {METADATA_PATH}")
    print(f"Informe: {REPORT_PATH}")
    print(f"Predicciones: {PREDICTIONS_PATH}")
    print("Modelo recargado y verificado.")
    print("V11 permanece intacto.")


if __name__ == "__main__":
    main()
