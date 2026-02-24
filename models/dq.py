"""
N-BeMod — Data Quality v0
20 checks sobre el dataset de loans curado.
"""
from datetime import datetime
from typing import Any, Dict, List
import pandas as pd
import numpy as np


def _check(name: str, result: str, detail: Any = None) -> dict:
    return {"check": name, "result": result, "detail": detail}


def run_dq_checks(df: pd.DataFrame, dataset_version_id: str) -> dict:
    checks: List[dict] = []
    REQUIRED = ["as_of_date", "entity_id", "portfolio", "product", "balance"]

    # 1. Required columns not null
    for col in REQUIRED:
        null_count = int(df[col].isna().sum()) if col in df.columns else len(df)
        result = "OK" if null_count == 0 else ("WARN" if null_count < len(df) * 0.05 else "KO")
        checks.append(_check(f"not_null_{col}", result, {"null_count": null_count, "total_rows": len(df)}))

    # 2. Balance > 0 and numeric
    if "balance" in df.columns:
        non_positive = int((pd.to_numeric(df["balance"], errors="coerce").fillna(0) <= 0).sum())
        result = "OK" if non_positive == 0 else ("WARN" if non_positive < len(df) * 0.01 else "KO")
        checks.append(_check("balance_positive", result, {"non_positive_count": non_positive}))
    else:
        checks.append(_check("balance_positive", "KO", {"detail": "column missing"}))

    # 3. Rate in reasonable range (-5% to 50%)
    if "rate" in df.columns:
        rates = pd.to_numeric(df["rate"], errors="coerce")
        out_of_range = int(((rates < -0.05) | (rates > 0.50)).sum())
        result = "OK" if out_of_range == 0 else ("WARN" if out_of_range < 10 else "KO")
        checks.append(_check("rate_in_range", result, {"out_of_range_count": out_of_range, "range": "[-5%, 50%]"}))
    else:
        checks.append(_check("rate_in_range", "WARN", {"detail": "rate column not present"}))

    # 4. Duplicates
    key_cols = ["as_of_date", "entity_id"]
    if "contract_id" in df.columns:
        key_cols.append("contract_id")
    else:
        key_cols += [c for c in ["portfolio", "segment", "product"] if c in df.columns]

    dup_count = int(df.duplicated(subset=key_cols).sum())
    result = "OK" if dup_count == 0 else ("WARN" if dup_count < len(df) * 0.01 else "KO")
    checks.append(_check("no_duplicates", result, {"duplicate_rows": dup_count, "key_cols": key_cols}))

    # 5. Column types: dates parseable
    if "as_of_date" in df.columns:
        try:
            pd.to_datetime(df["as_of_date"], errors="raise")
            checks.append(_check("as_of_date_parseable", "OK", None))
        except Exception as e:
            checks.append(_check("as_of_date_parseable", "WARN", {"error": str(e)}))

    # 6. % nulls per column
    for col in df.columns:
        pct = float(df[col].isna().mean())
        result = "OK" if pct == 0 else ("WARN" if pct < 0.05 else "KO")
        checks.append(_check(f"null_pct_{col}", result, {"null_pct": round(pct * 100, 2)}))

    # 7. Minimum row count
    row_count = len(df)
    result = "OK" if row_count >= 10 else ("WARN" if row_count >= 1 else "KO")
    checks.append(_check("min_row_count", result, {"row_count": row_count}))

    # 8. Segment column present
    seg_present = "segment" in df.columns and df["segment"].notna().all()
    checks.append(_check("segment_present", "OK" if seg_present else "WARN", None))

    # 9. Balance numeric type
    if "balance" in df.columns:
        non_numeric = pd.to_numeric(df["balance"], errors="coerce").isna().sum()
        result = "OK" if non_numeric == 0 else "KO"
        checks.append(_check("balance_numeric", result, {"non_numeric": int(non_numeric)}))

    # 10. Outliers (IQR) on balance
    if "balance" in df.columns:
        bal = pd.to_numeric(df["balance"], errors="coerce").dropna()
        q1, q3 = bal.quantile(0.25), bal.quantile(0.75)
        iqr = q3 - q1
        outliers = int(((bal < q1 - 3 * iqr) | (bal > q3 + 3 * iqr)).sum())
        result = "OK" if outliers == 0 else ("WARN" if outliers < len(df) * 0.02 else "KO")
        checks.append(_check("balance_outliers_iqr", result, {"outlier_count": outliers}))

    ko = sum(1 for c in checks if c["result"] == "KO")
    warn = sum(1 for c in checks if c["result"] == "WARN")
    ok = sum(1 for c in checks if c["result"] == "OK")

    return {
        "dataset_version_id": dataset_version_id,
        "timestamp": datetime.utcnow().isoformat(),
        "total_rows": row_count,
        "summary": {"OK": ok, "WARN": warn, "KO": ko},
        "overall_status": "KO" if ko > 0 else ("WARN" if warn > 0 else "OK"),
        "checks": checks,
    }
