from pathlib import Path

import polars as pl
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    precision_score,
    recall_score,
    f1_score,
)

DATA_FILE = Path("experiments/synthetic_data/cdr_experiment_v3.parquet")
RESULTS_DIR = Path("experiments/results")
REPORT_FILE = RESULTS_DIR / "rules_evaluation_v1.txt"


def main():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"No se encuentra el dataset: {DATA_FILE}")

    df = pl.read_parquet(DATA_FILE)

    required = {
        "call_id",
        "caller",
        "calls_last_5m",
        "unique_destinations_last_24h",
        "is_suspicious",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas obligatorias: {sorted(missing)}")

    expected = (
        (pl.col("calls_last_5m") >= 5)
        & (pl.col("unique_destinations_last_24h") >= 4)
    )

    evaluated = df.with_columns(
        expected.alias("predicted_suspicious")
    )

    y_true = evaluated["is_suspicious"].cast(pl.Int8).to_numpy()
    y_pred = evaluated["predicted_suspicious"].cast(pl.Int8).to_numpy()

    tn, fp, fn, tp = confusion_matrix(
        y_true, y_pred, labels=[0, 1]
    ).ravel()

    report = f"""
SENTINELSTREAM - EXPERIMENTO A
Detector basado en reglas - V1
========================================

Dataset: {DATA_FILE}
Registros evaluados: {len(df)}
Abonados únicos: {df["caller"].n_unique()}
Registros sospechosos reales: {int(y_true.sum())}
Registros sospechosos predichos: {int(y_pred.sum())}

REGLAS
------
1. calls_last_5m >= 5
2. unique_destinations_last_24h >= 4
3. Ambas condiciones deben cumplirse.

MATRIZ DE CONFUSIÓN
-------------------
Verdaderos negativos (TN): {tn}
Falsos positivos (FP): {fp}
Falsos negativos (FN): {fn}
Verdaderos positivos (TP): {tp}

MÉTRICAS
--------
Accuracy:  {accuracy_score(y_true, y_pred):.4f}
Precision: {precision_score(y_true, y_pred, zero_division=0):.4f}
Recall:    {recall_score(y_true, y_pred, zero_division=0):.4f}
F1-score:  {f1_score(y_true, y_pred, zero_division=0):.4f}

INFORME POR CLASE
-----------------
{classification_report(y_true, y_pred, labels=[0, 1], target_names=["normal", "sospechosa"], zero_division=0)}

LIMITACIONES
------------
Las etiquetas de este dataset sintético fueron generadas usando
la misma regla que se evalúa aquí. Por tanto, este resultado valida
la implementación de la regla frente a las etiquetas sintéticas;
no demuestra capacidad para detectar fraude real.

Las métricas corresponden exclusivamente al dataset evaluado.
"""

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    if REPORT_FILE.exists():
        raise FileExistsError(
            f"El informe ya existe y no será sobrescrito: {REPORT_FILE}"
        )

    REPORT_FILE.write_text(report.strip() + "\n", encoding="utf-8")

    print(report)
    print(f"Informe guardado en: {REPORT_FILE}")


if __name__ == "__main__":
    main()
