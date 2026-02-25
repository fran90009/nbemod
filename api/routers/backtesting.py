"""N-BeMod — Backtesting router"""
import io
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from db.session import get_db
from db.models import ModelVersion, DatasetVersion
from storage import minio_client as minio
from models.backtesting import run_backtesting
import pandas as pd

logger = logging.getLogger(__name__)
router = APIRouter()


class BacktestRequest(BaseModel):
    model_version_id: str
    noise_std: Optional[float] = 0.02


class BacktestOut(BaseModel):
    model_version_id: str
    global_metrics: dict
    segment_metrics: dict


@router.post("/run", response_model=BacktestOut)
def run_backtest(payload: BacktestRequest, db: Session = Depends(get_db)):
    mv = db.query(ModelVersion).filter(ModelVersion.id == payload.model_version_id).first()
    if not mv:
        raise HTTPException(404, "ModelVersion not found")
    if mv.status != "SUCCEEDED":
        raise HTTPException(422, f"ModelVersion status is {mv.status} — must be SUCCEEDED")

    dv = mv.dataset_version

    # Cargar datos
    loan_bytes = minio.download_bytes(str(dv.curated_path))
    loans_df = pd.read_parquet(io.BytesIO(loan_bytes))

    curves_bytes = minio.download_bytes(str(mv.curves_path))
    curves_df = pd.read_parquet(io.BytesIO(curves_bytes))

    # Ejecutar backtesting
    results_df, metrics = run_backtesting(loans_df, curves_df, mv.params_json)

    # Guardar resultados en MinIO
    entity_id = str(dv.entity_id)
    asof = str(dv.as_of_date)
    dv_id = str(dv.id)
    mv_id = str(mv.id)

    bt_path = f"entity={entity_id}/asof={asof}/dataset_version={dv_id}/model_version={mv_id}/prepay_curve/backtesting.parquet"
    minio.upload_bytes(results_df.to_parquet(index=False), bt_path)

    bt_metrics_path = f"entity={entity_id}/asof={asof}/dataset_version={dv_id}/model_version={mv_id}/prepay_curve/backtesting_metrics.json"
    minio.upload_json(metrics, bt_metrics_path)

    logger.info(f"Backtesting complete | mv_id={mv_id} | MAPE={metrics['global']['mape']}%")

    return BacktestOut(
        model_version_id=mv_id,
        global_metrics=metrics["global"],
        segment_metrics=metrics["segments"],
    )


@router.get("/results/{model_version_id}", response_model=BacktestOut)
def get_backtest_results(model_version_id: str, db: Session = Depends(get_db)):
    mv = db.query(ModelVersion).filter(ModelVersion.id == model_version_id).first()
    if not mv:
        raise HTTPException(404, "ModelVersion not found")

    dv = mv.dataset_version
    entity_id = str(dv.entity_id)
    asof = str(dv.as_of_date)
    dv_id = str(dv.id)

    bt_metrics_path = f"entity={entity_id}/asof={asof}/dataset_version={dv_id}/model_version={model_version_id}/prepay_curve/backtesting_metrics.json"

    try:
        import json
        data = minio.download_bytes(bt_metrics_path)
        metrics = json.loads(data)
    except Exception:
        raise HTTPException(404, "Backtesting results not found. Run backtesting first.")

    return BacktestOut(
        model_version_id=model_version_id,
        global_metrics=metrics["global"],
        segment_metrics=metrics["segments"],
    )