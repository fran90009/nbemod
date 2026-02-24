"""
N-BeMod — Prepay Curve Model (Simple Average CPR/SMM por segmento)

CPR (Conditional Prepayment Rate): tasa anual de prepago
SMM (Single Monthly Mortality): tasa mensual equivalente
Relación: SMM = 1 - (1 - CPR)^(1/12)
           CPR = 1 - (1 - SMM)^12

El modelo estima CPR por segmento a partir del histórico o,
en MVP sin histórico suficiente, aplica curvas calibradas con
distribución empírica simple sobre el balance.
"""
import logging
import warnings
import numpy as np
import pandas as pd
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


def cpr_to_smm(cpr: float) -> float:
    """CPR anual → SMM mensual."""
    return 1 - (1 - max(cpr, 0)) ** (1 / 12)


def smm_to_cpr(smm: float) -> float:
    """SMM mensual → CPR anual."""
    return 1 - (1 - max(smm, 0)) ** 12


def calibrate_simple_average(
    df: pd.DataFrame,
    config: dict,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Calibra curvas CPR/SMM por segmento usando simple average.

    Si el dataset no tiene histórico de prepagos observados,
    genera una curva CPR proxy basada en:
      - Seasoning ramp: CPR crece los primeros 30 meses (PSA-inspired)
      - Nivel de CPR calibrado en función del tipo de portfolio (rate)
      - Aplica smoothing opcional (rolling average)

    Returns:
        curves_df: DataFrame con columnas [segment, month, cpr, smm]
        metrics: dict resumen
    """
    horizon_months = int(config.get("horizon_months", 60))
    smoothing = bool(config.get("smoothing", False))
    min_segment_size = int(config.get("min_segment_size", 10))

    warnings.filterwarnings("ignore")

    df = df.copy()
    df["balance"] = pd.to_numeric(df["balance"], errors="coerce").fillna(0)
    df["rate"] = pd.to_numeric(df.get("rate", pd.Series(dtype=float)), errors="coerce").fillna(0.03)

    segments = df["segment"].dropna().unique()
    records = []
    metrics = {"segments": {}, "global": {}}

    for seg in segments:
        seg_df = df[df["segment"] == seg]
        n = len(seg_df)
        size_warn = n < min_segment_size

        # Estimated CPR base from average rate
        avg_rate = float(seg_df["rate"].mean())
        total_balance = float(seg_df["balance"].sum())

        # Base CPR heuristic: higher rate → higher prepay incentive (simplified)
        # In production: replace with regression on observed prepay data
        cpr_base = max(0.05, min(0.40, avg_rate * 2.5 + 0.05))

        # PSA-inspired seasoning ramp over first 30 months
        months = np.arange(1, horizon_months + 1)
        ramp = np.minimum(months / 30.0, 1.0)
        cpr_curve = cpr_base * ramp

        if smoothing and len(cpr_curve) > 3:
            cpr_curve = pd.Series(cpr_curve).rolling(3, min_periods=1, center=True).mean().values

        smm_curve = np.array([cpr_to_smm(c) for c in cpr_curve])

        for i, m in enumerate(months):
            records.append({
                "segment": seg,
                "month": int(m),
                "cpr": round(float(cpr_curve[i]), 6),
                "smm": round(float(smm_curve[i]), 6),
            })

        metrics["segments"][str(seg)] = {
            "n_contracts": int(n),
            "total_balance": round(total_balance, 2),
            "avg_rate": round(avg_rate, 4),
            "cpr_base": round(cpr_base, 4),
            "size_warn": size_warn,
        }

        if size_warn:
            logger.warning(f"Segment '{seg}' has only {n} contracts (min_segment_size={min_segment_size})")

    curves_df = pd.DataFrame(records)

    metrics["global"] = {
        "total_segments": int(len(segments)),
        "horizon_months": horizon_months,
        "total_balance": round(float(df["balance"].sum()), 2),
        "total_contracts": int(len(df)),
        "curve_method": config.get("curve_method", "simple_average"),
    }

    logger.info(f"Calibration complete: {len(segments)} segments, {horizon_months} months")
    return curves_df, metrics
