from collections import deque
from datetime import datetime

import numpy as np
import polars as pl
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

DATA_PATH = "experiments/synthetic_data/cdr_experiment_v10.parquet"
REPORT_PATH = "experiments/results/anomaly_evaluation_v11.txt"
PREDICTIONS_PATH = "experiments/results/anomaly_predictions_v11.parquet"

BASE_FEATURES = [
    "prior_calls_1m",
    "prior_calls_5m",
    "prior_calls_15m",
    "prior_unique_destinations_24h",
]
NEW_FEATURE = "is_new_destination_24h"
ENHANCED_FEATURES = BASE_FEATURES + [NEW_FEATURE]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def add_destination_novelty(df):
    """Marca si el destino no apareció en las 24 h anteriores del llamante."""
    result = []

    for caller_df in df.partition_by("caller", maintain_order=True):
        history = deque()
        seen = {}

        for row in caller_df.iter_rows(named=True):
            timestamp = datetime.fromisoformat(row["timestamp"]).timestamp()

            while history and timestamp - history[0][0] > 86400:
                _, old_callee = history.popleft()
                seen[old_callee] -= 1
                if seen[old_callee] == 0:
                    del seen[old_callee]

            result.append({
                "call_id": row["call_id"],
                NEW_FEATURE: int(row["callee"] not in seen),
            })

            callee = row["callee"]
            history.append((timestamp, callee))
            seen[callee] = seen.get(callee, 0) + 1

    feature_df = pl.DataFrame(result)
    return df.join(feature_df, on="call_id", how="left", validate="1:1")


def choose_threshold(y_true, scores):
    """Maximiza F1 en calibración; desempata por mayor precisión."""
    candidates = np.unique(scores)
    best = None

    for threshold in candidates:
        predictions = (scores >= threshold).astype(int)
        f1 = f1_score(y_true, predictions, zero_division=0)
        precision = precision_score(
            y_true, predictions, zero_division=0
        )

        candidate = (f1, precision, float(threshold))
        if best is None or candidate[:2] > best[:2]:
            best = candidate

    return best[2], best[0], best[1]


def evaluate_model(name, features, train_df, calibration_df, test_df):
    model = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,
        random_state=SEED,
        n_jobs=-1,
    )

    model.fit(train_df.select(features).to_numpy())

    calibration_scores = -model.decision_function(
        calibration_df.select(features).to_numpy()
    )
    y_calibration = calibration_df["is_burst_event"].cast(
        pl.Int8
    ).to_numpy()

    threshold, calibration_f1, calibration_precision = choose_threshold(
        y_calibration, calibration_scores
    )

    test_scores = -model.decision_function(
        test_df.select(features).to_numpy()
    )
    y_true = test_df["is_burst_event"].cast(pl.Int8).to_numpy()
    y_pred = (test_scores >= threshold).astype(int)

    tn, fp, fn, tp = confusion_matrix(
        y_true, y_pred, labels=[0, 1]
    ).ravel()

    metrics = {
        "model": name,
        "features": features,
        "threshold": threshold,
        "calibration_f1": calibration_f1,
        "calibration_precision": calibration_precision,
        "accuracy": accuracy_score(y_true, y_pred),
        "precision": precision_score(y_true, y_pred, zero_division=0),
        "recall": recall_score(y_true, y_pred, zero_division=0),
        "f1": f1_score(y_true, y_pred, zero_division=0),
        "pr_auc": average_precision_score(y_true, test_scores),
        "roc_auc": roc_auc_score(y_true, test_scores),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "tp": int(tp),
        "test_calls": len(y_true),
        "test_callers": test_df["caller"].n_unique(),
        "test_positives": int(y_true.sum()),
        "predicted_positives": int(y_pred.sum()),
    }

    predictions = test_df.select([
        "call_id",
        "caller",
        "timestamp",
        "is_burst_event",
        "burst_id",
        "burst_call_index",
    ]).with_columns([
        pl.Series(f"{name}_score", test_scores),
        pl.Series(f"{name}_prediction", y_pred),
        pl.lit(threshold).alias(f"{name}_threshold"),
    ])

    return metrics, predictions


def format_metrics(m):
    lines = [
        f"Modelo: {m['model']}",
        f"Características: {', '.join(m['features'])}",
        f"Umbral seleccionado en calibración: {m['threshold']:.8f}",
        f"F1 calibración: {m['calibration_f1']:.4f}",
        f"Precisión calibración: {m['calibration_precision']:.4f}",
        f"Llamadas de prueba: {m['test_calls']}",
        f"Llamantes de prueba: {m['test_callers']}",
        f"Positivos reales: {m['test_positives']}",
        f"Predicciones positivas: {m['predicted_positives']}",
        f"TN={m['tn']} FP={m['fp']} FN={m['fn']} TP={m['tp']}",
        f"Accuracy: {m['accuracy']:.4f}",
        f"Precision: {m['precision']:.4f}",
        f"Recall: {m['recall']:.4f}",
        f"F1: {m['f1']:.4f}",
        f"PR-AUC (Average Precision): {m['pr_auc']:.4f}",
        f"ROC-AUC: {m['roc_auc']:.4f}",
    ]
    return "\n".join(lines)


def main():
    # No sobrescribir artefactos anteriores.
    existing = [
        path for path in [REPORT_PATH, PREDICTIONS_PATH]
        if __import__("os").path.exists(path)
    ]
    require(not existing, f"Preflight: ya existen archivos V11: {existing}")
    require(
        __import__("os").path.exists(DATA_PATH),
        f"No existe el dataset de entrada: {DATA_PATH}",
    )

    df = pl.read_parquet(DATA_PATH)

    required = [
        "call_id", "caller", "timestamp", "callee",
        "is_burst_event", "burst_id", "burst_call_index",
        *BASE_FEATURES,
    ]
    missing = [column for column in required if column not in df.columns]
    require(not missing, f"Faltan columnas: {missing}")
    require(df.height == 10000, f"Filas inesperadas: {df.height}")
    require(df["call_id"].n_unique() == df.height, "call_id no es único")
    # Las columnas opcionales del episodio pueden ser nulas en llamadas normales.
    required_non_null = [
        "call_id", "caller", "timestamp", "callee",
        "is_burst_event", *BASE_FEATURES,
    ]

    null_counts = df.select([
        pl.col(column).null_count().alias(column)
        for column in required_non_null
    ])

    require(
        all(
            null_counts.row(0)[i] == 0
            for i in range(len(required_non_null))
        ),
        "Hay valores nulos en columnas obligatorias",
    )

    require(
        df.filter(pl.col("is_burst_event"))["burst_id"].null_count() == 0,
        "Hay burst_id nulos en llamadas de episodio",
    )
    require(
        df.filter(pl.col("is_burst_event"))["burst_call_index"].null_count() == 0,
        "Hay burst_call_index nulos en llamadas de episodio",
    )
    require(
        df.filter(~pl.col("is_burst_event"))["burst_id"].null_count()
        == df.filter(~pl.col("is_burst_event")).height,
        "Se esperaba burst_id nulo en llamadas normales",
    )
    require(
        df.filter(~pl.col("is_burst_event"))["burst_call_index"].null_count()
        == df.filter(~pl.col("is_burst_event")).height,
        "Se esperaba burst_call_index nulo en llamadas normales",
    )

    # Orden cronológico determinista, con call_id como desempate.
    df = df.sort(["caller", "timestamp", "call_id"])
    df = add_destination_novelty(df)

    require(df[NEW_FEATURE].null_count() == 0, "Nulos en la nueva característica")
    require(
        set(df[NEW_FEATURE].unique().to_list()).issubset({0, 1}),
        "La nueva característica no es binaria",
    )

    callers = sorted(df["caller"].unique().to_list())
    require(len(callers) == 500, f"Cantidad de llamantes inesperada: {len(callers)}")

    rng = np.random.default_rng(SEED)
    shuffled = np.array(callers, dtype=object)
    rng.shuffle(shuffled)

    train_callers = set(shuffled[:300].tolist())
    calibration_callers = set(shuffled[300:400].tolist())
    test_callers = set(shuffled[400:].tolist())

    require(
        not (train_callers & calibration_callers
             or train_callers & test_callers
             or calibration_callers & test_callers),
        "Hay llamantes compartidos entre particiones",
    )

    train_df = df.filter(pl.col("caller").is_in(list(train_callers)))
    calibration_df = df.filter(
        pl.col("caller").is_in(list(calibration_callers))
    )
    test_df = df.filter(pl.col("caller").is_in(list(test_callers)))

    require(
        train_df.height + calibration_df.height + test_df.height == df.height,
        "La partición perdió o duplicó llamadas",
    )
    require(
        train_df["is_burst_event"].sum() > 0
        and calibration_df["is_burst_event"].sum() > 0
        and test_df["is_burst_event"].sum() > 0,
        "Alguna partición no tiene positivos",
    )

    baseline, pred_base = evaluate_model(
        "baseline", BASE_FEATURES, train_df, calibration_df, test_df
    )
    enhanced, pred_enhanced = evaluate_model(
        "enhanced", ENHANCED_FEATURES, train_df, calibration_df, test_df
    )

    # Unir ambos modelos por call_id, manteniendo una fila por llamada.
    predictions = pred_base.join(
        pred_enhanced.drop([
            "caller", "timestamp", "is_burst_event",
            "burst_id", "burst_call_index",
        ]),
        on="call_id",
        how="inner",
        validate="1:1",
    )

    require(predictions.height == test_df.height, "Error al unir predicciones")

    novelty_rates = (
        df.group_by("is_burst_event")
        .agg([
            pl.len().alias("calls"),
            pl.col(NEW_FEATURE).mean().alias("new_destination_rate"),
        ])
        .sort("is_burst_event")
    )

    report = [
        "SENTINELSTREAM — EXPERIMENTO V11",
        "================================",
        "Dataset: cdr_experiment_v10.parquet (datos sintéticos)",
        "Semilla: 42",
        "IsolationForest: 300 estimadores; contamination=0.05",
        "Partición por llamante: 300 entrenamiento / 100 calibración / 100 prueba",
        "La novedad del destino usa solo eventos previos dentro de las 24 horas.",
        "El umbral se selecciona únicamente en calibración.",
        "",
        "AUDITORÍA DE LA CARACTERÍSTICA NUEVA",
        str(novelty_rates),
        "",
        "MODELO BASE (4 características)",
        format_metrics(baseline),
        "",
        "MODELO AMPLIADO (5 características)",
        format_metrics(enhanced),
        "",
        "DIFERENCIA (AMPLIADO - BASE)",
    ]

    for metric in [
        "precision", "recall", "f1", "pr_auc", "roc_auc", "fp", "fn"
    ]:
        delta = enhanced[metric] - baseline[metric]
        report.append(f"{metric}: {delta:+.4f}")

    report.extend([
        "",
        "LIMITACIONES",
        "- Los datos y las etiquetas son sintéticos.",
        "- La mejora en datos sintéticos no demuestra rendimiento en producción.",
        "- La característica de novedad puede tener señal limitada.",
        "- El score de IsolationForest no es una probabilidad calibrada.",
        "- No se guarda ningún modelo serializado.",
    ])

    # Escribir solo después de completar las validaciones y ambos entrenamientos.
    with open(REPORT_PATH, "x", encoding="utf-8") as handle:
        handle.write("\n".join(report) + "\n")

    try:
        predictions.write_parquet(PREDICTIONS_PATH)
    except Exception:
        # Evitar dejar un informe que aparente un experimento completo.
        import os
        os.remove(REPORT_PATH)
        raise

    print("\n".join(report))
    print(f"\nInforme guardado: {REPORT_PATH}")
    print(f"Predicciones guardadas: {PREDICTIONS_PATH}")


if __name__ == "__main__":
    main()
