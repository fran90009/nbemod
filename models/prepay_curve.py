"""
N-BeMod — Prepay Curve Model (Simple Average CPR/SMM por segmento)

CPR (Conditional Prepayment Rate): tasa anual de prepago
SMM (Single Monthly Mortality): tasa mensual equivalente
Relación: SMM = 1 - (1 - CPR)^(1/12)
           CPR = 1 - (1 - SMM)^12

Efecto de escenarios de tipos:
  - Shock +100bps → incrementa CPR (mayor incentivo a refinanciar)
  - Shock -100bps → reduce CPR (menor incentivo a refinanciar)
  - Elasticidad por defecto: 0.2 (20% de variación por cada 100bps)
"""
import logging
import warnings
import numpy as np
import pandas as pd
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

# Elasticidad CPR respecto a tipos (configurable)
# +100bps → CPR * (1 + ELASTICITY)
# -100bps → CPR * (1 - ELASTICITY)
DEFAULT_ELASTICITY = 0.20


def cpr_to_smm(cpr: float) -> float:
    return 1 - (1 - max(cpr, 0)) ** (1 / 12)


def smm_to_cpr(smm: float) -> float:
    return 1 - (1 - max(smm, 0)) ** 12


def apply_rate_shock(cpr_base: float, rate_shock_bps: int, elasticity: float = DEFAULT_ELASTICITY) -> float:
    """
    Aplica el efecto de un shock de tipos sobre el CPR base.
    
    Lógica: un shock positivo (subida de tipos) aumenta el CPR
    porque los prestatarios con tipos variables tienen mayor
    incentivo a refinanciar a tipos fijos más bajos.
    
    rate_shock_bps: shock en puntos básicos (100 = +1%)
    elasticity: % de variación del CPR por cada 100bps
    """
    shock_factor = 1 + (rate_shock_bps / 100) * elasticity
    return max(0.0, min(1.0, cpr_base * shock_factor))


def calibrate_simple_average(
    df: pd.DataFrame,
    config: dict,
    rate_shock_bps: int = 0,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Calibra curvas CPR/SMM por segmento usando simple average.
    Aplica el efecto del escenario de tipos si rate_shock_bps != 0.
    """
    horizon_months = int(config.get("horizon_months", 60))
    smoothing = bool(config.get("smoothing", False))
    min_segment_size = int(config.get("min_segment_size", 10))
    elasticity = float(config.get("elasticity", DEFAULT_ELASTICITY))

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

        avg_rate = float(seg_df["rate"].mean())
        total_balance = float(seg_df["balance"].sum())

        # CPR base (sin shock)
        cpr_base = max(0.05, min(0.40, avg_rate * 2.5 + 0.05))

        # Aplicar shock de tipos
        cpr_shocked = apply_rate_shock(cpr_base, rate_shock_bps, elasticity)

        # PSA-inspired seasoning ramp
        months = np.arange(1, horizon_months + 1)
        ramp = np.minimum(months / 30.0, 1.0)
        cpr_curve = cpr_shocked * ramp

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
            "cpr_shocked": round(cpr_shocked, 4),
            "rate_shock_bps": rate_shock_bps,
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
        "rate_shock_bps": rate_shock_bps,
        "elasticity": elasticity,
    }

    logger.info(f"Calibration complete: {len(segments)} segments, {horizon_months} months, shock={rate_shock_bps}bps")
    return curves_df, metrics
