"""
N-BeMod — Backtesting v0
Validación rolling del modelo prepay_curve.
Métricas: MAPE y WAPE por segmento y global.
"""
import logging
import numpy as np
import pandas as pd
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


def compute_mape(actual: np.ndarray, predicted: np.ndarray, min_threshold: float = 0.02) -> float:
    """
    Mean Absolute Percentage Error.
    Excluye meses con CPR < min_threshold para evitar distorsión
    del seasoning ramp (CPR cercano a 0 en meses iniciales).
    """
    mask = actual >= min_threshold
    if mask.sum() == 0:
        return np.nan
    return float(np.mean(np.abs((actual[mask] - predicted[mask]) / actual[mask])) * 100)
    
def compute_wape(actual: np.ndarray, predicted: np.ndarray, weights: np.ndarray) -> float:
    """Weighted Absolute Percentage Error."""
    denominator = np.sum(actual * weights)
    if denominator == 0:
        return np.nan
    return float(np.sum(np.abs(actual - predicted) * weights) / denominator * 100)


def simulate_observed_cpr(
    curves_df: pd.DataFrame,
    noise_std: float = 0.02,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Simula CPR observado añadiendo ruido gaussiano al CPR predicho.
    En producción: reemplazar por CPR real calculado desde datos históricos.
    
    noise_std: desviación estándar del ruido (default 2%)
    """
    np.random.seed(seed)
    observed = curves_df.copy()
    noise = np.random.normal(0, noise_std, len(observed))
    observed["cpr_observed"] = np.clip(observed["cpr"] + noise, 0.001, 0.999)
    observed["smm_observed"] = 1 - (1 - observed["cpr_observed"]) ** (1 / 12)
    return observed


def run_backtesting(
    loans_df: pd.DataFrame,
    curves_df: pd.DataFrame,
    config: dict,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Ejecuta el backtesting comparando CPR predicho vs observado.

    Returns:
        results_df: DataFrame con columnas:
            [segment, month, cpr_predicted, cpr_observed, abs_error, pct_error]
        metrics: dict con MAPE y WAPE por segmento y global
    """
    # Simular CPR observado (en producción: datos reales)
    observed_df = simulate_observed_cpr(curves_df)

    # Balance por segmento para ponderar WAPE
    seg_balance = loans_df.groupby("segment")["balance"].sum().to_dict()

    results = []
    metrics = {"segments": {}, "global": {}}

    segments = observed_df["segment"].unique()

    all_actual = []
    all_predicted = []
    all_weights = []

    for seg in segments:
        seg_df = observed_df[observed_df["segment"] == seg].copy()
        actual = seg_df["cpr_observed"].values
        predicted = seg_df["cpr"].values
        balance = float(seg_balance.get(seg, 1.0))

        mape = compute_mape(actual, predicted)
        wape = compute_wape(actual, predicted, np.ones(len(actual)))

        metrics["segments"][str(seg)] = {
            "mape": round(mape, 4),
            "wape": round(wape, 4),
            "balance": round(balance, 2),
            "n_months": len(seg_df),
            "avg_cpr_predicted": round(float(predicted.mean()), 4),
            "avg_cpr_observed": round(float(actual.mean()), 4),
        }

        for _, row in seg_df.iterrows():
            abs_err = abs(row["cpr_observed"] - row["cpr"])
            pct_err = abs_err / row["cpr_observed"] * 100 if row["cpr_observed"] > 0 else np.nan
            results.append({
                "segment": seg,
                "month": int(row["month"]),
                "cpr_predicted": round(float(row["cpr"]), 6),
                "cpr_observed": round(float(row["cpr_observed"]), 6),
                "abs_error": round(float(abs_err), 6),
                "pct_error": round(float(pct_err), 4) if not np.isnan(pct_err) else None,
            })

        all_actual.extend(actual.tolist())
        all_predicted.extend(predicted.tolist())
        all_weights.extend([balance] * len(actual))

    all_actual = np.array(all_actual)
    all_predicted = np.array(all_predicted)
    all_weights = np.array(all_weights)

    global_mape = compute_mape(all_actual, all_predicted)
    global_wape = compute_wape(all_actual, all_predicted, all_weights)

    metrics["global"] = {
        "mape": round(global_mape, 4),
        "wape": round(global_wape, 4),
        "total_segments": len(segments),
        "total_months": len(results),
        "interpretation": _interpret_mape(global_mape),
    }

    results_df = pd.DataFrame(results)
    logger.info(f"Backtesting complete | MAPE={global_mape:.2f}% | WAPE={global_wape:.2f}%")
    return results_df, metrics


def _interpret_mape(mape: float) -> str:
    """Interpretación cualitativa del MAPE según estándares ALM."""
    if mape < 5:
        return "EXCELLENT — Model fit is very strong"
    elif mape < 10:
        return "GOOD — Model fit is acceptable for ALM use"
    elif mape < 20:
        return "FAIR — Consider recalibration or segmentation review"
    else:
        return "POOR — Model requires significant revision"