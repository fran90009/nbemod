"""N-BeMod — Models router"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from db.session import get_db
from db.models import ModelVersion, ModelDefinition, DatasetVersion
from worker.tasks import task_calibrate_prepay_curve

logger = logging.getLogger(__name__)
router = APIRouter()


class CalibrateRequest(BaseModel):
    dataset_version_id: str
    curve_method: str = "simple_average"   # simple_average | cohort
    horizon_months: int = 60
    time_bucket: str = "monthly"
    min_segment_size: int = 10
    smoothing: bool = False


class ModelVersionOut(BaseModel):
    id: str
    model_definition_id: str
    dataset_version_id: str
    params_json: dict
    status: str
    summary_metrics: Optional[dict]
    curves_path: Optional[str]
    error_message: Optional[str]
    created_at: str


@router.post("/prepay_curve/calibrate", status_code=202)
def calibrate_prepay_curve(payload: CalibrateRequest, db: Session = Depends(get_db)):
    dv = db.query(DatasetVersion).filter(DatasetVersion.id == payload.dataset_version_id).first()
    if not dv:
        raise HTTPException(404, "DatasetVersion not found")
    if dv.status not in ("OK", "WARN"):
        raise HTTPException(422, f"Dataset status is {dv.status} — must be OK or WARN to calibrate")

    # Get or create ModelDefinition
    md = db.query(ModelDefinition).filter(ModelDefinition.name == "prepay_curve").first()
    if not md:
        md = ModelDefinition(
            name="prepay_curve",
            version="1.0",
            description="Simple average CPR/SMM curve by segment",
            config_schema_json={"type": "object"},
        )
        db.add(md)
        db.commit()
        db.refresh(md)

    mv = ModelVersion(
        model_definition_id=md.id,
        dataset_version_id=payload.dataset_version_id,
        params_json=payload.dict(),
        status="QUEUED",
    )
    db.add(mv)
    db.commit()
    db.refresh(mv)

    task_calibrate_prepay_curve.delay(str(mv.id))
    logger.info(f"Calibration queued | mv_id={mv.id} | dv_id={payload.dataset_version_id}")
    return {"model_version_id": mv.id, "status": "QUEUED"}


@router.get("/versions", response_model=List[ModelVersionOut])
def list_model_versions(db: Session = Depends(get_db)):
    mvs = db.query(ModelVersion).order_by(ModelVersion.created_at.desc()).all()
    return [_mv_out(mv) for mv in mvs]


@router.get("/versions/{model_version_id}", response_model=ModelVersionOut)
def get_model_version(model_version_id: str, db: Session = Depends(get_db)):
    mv = db.query(ModelVersion).filter(ModelVersion.id == model_version_id).first()
    if not mv:
        raise HTTPException(404, "ModelVersion not found")
    return _mv_out(mv)


def _mv_out(mv: ModelVersion) -> ModelVersionOut:
    return ModelVersionOut(
        id=mv.id,
        model_definition_id=mv.model_definition_id,
        dataset_version_id=mv.dataset_version_id,
        params_json=mv.params_json,
        status=mv.status,
        summary_metrics=mv.summary_metrics,
        curves_path=mv.curves_path,
        error_message=mv.error_message,
        created_at=str(mv.created_at),
    )
