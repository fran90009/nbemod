"""
N-BeMod — Cashflow Engine
Genera cashflows mensuales por segmento aplicando la curva SMM al balance inicial.
"""
import logging
import numpy as np
import pandas as pd
from typing import Dict

logger = logging.getLogger(__name__)


def compute_cashflows(
    loans_df: pd.DataFrame,
    curves_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """
    Genera tabla de cashflows mensuales por segmento.

    Método simplificado MVP:
      opening_balance[t] = opening_balance[t-1] - prepayment[t-1]
      prepayment[t] = smm[t] * opening_balance[t]
      closing_balance[t] = opening_balance[t] - prepayment[t]

    Returns:
        DataFrame con columnas:
        [segment, month, opening_balance, smm, prepayment, closing_balance]
    """
    loans_df = loans_df.copy()
    loans_df["balance"] = pd.to_numeric(loans_df["balance"], errors="coerce").fillna(0)

    # Aggregate balance by segment
    seg_balance = loans_df.groupby("segment")["balance"].sum().reset_index()
    seg_balance.columns = ["segment", "initial_balance"]

    horizon_months = int(config.get("horizon_months", 60))

    records = []

    for _, row in seg_balance.iterrows():
        seg = row["segment"]
        init_bal = float(row["initial_balance"])

        seg_curves = curves_df[curves_df["segment"] == seg].sort_values("month")
        if seg_curves.empty:
            logger.warning(f"No curve found for segment '{seg}', skipping")
            continue

        opening = init_bal
        for _, c_row in seg_curves.iterrows():
            month = int(c_row["month"])
            smm = float(c_row["smm"])
            prepayment = smm * opening
            closing = opening - prepayment

            records.append({
                "segment": seg,
                "month": month,
                "opening_balance": round(opening, 2),
                "smm": round(smm, 6),
                "cpr": round(float(c_row["cpr"]), 6),
                "prepayment": round(prepayment, 2),
                "closing_balance": round(max(closing, 0), 2),
            })

            opening = max(closing, 0)
            if opening < 1.0:
                break

    cashflows_df = pd.DataFrame(records)

    total_prepay = cashflows_df["prepayment"].sum() if not cashflows_df.empty else 0
    logger.info(f"Cashflows computed: {len(cashflows_df)} rows | total_prepayment={total_prepay:,.0f}")

    return cashflows_df
