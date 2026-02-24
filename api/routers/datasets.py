"""N-BeMod — Datasets router"""
import hashlib
import logging
from datetime import date
from typing import List, Optional
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

from db.session import get_db
from db.models import DatasetVersion, Entity
from storage import minio_client as minio
from worker.tasks import task_normalize_and_dq

logger = logging.getLogger(__name__)
router = APIRouter()


class DatasetVersionOut(BaseModel):
    id: str
    entity_id: str
    as_of_date: str
    source_name: str
    file_hash: str
    status: str
    row_count: Optional[int]
    total_balance: Optional[float]
    dq_summary: Optional[dict]
    created_at: str


@router.post("/loans/upload", status_code=202)
async def upload_loans(
    file: UploadFile = File(...),
    entity_id: str = Form(...),
    as_of_date: str = Form(...),  # YYYY-MM-DD
    db: Session = Depends(get_db),
):
    entity = db.query(Entity).filter(Entity.id == entity_id).first()
    if not entity:
        raise HTTPException(404, "Entity not found")

    content = await file.read()
    file_hash = hashlib.sha256(content).hexdigest()

    dv = DatasetVersion(
        entity_id=entity_id,
        as_of_date=date.fromisoformat(as_of_date),
        source_name=file.filename,
        file_hash=file_hash,
        status="PENDING",
    )
    db.add(dv)
    db.commit()
    db.refresh(dv)

    # Upload RAW to MinIO
    raw_p = minio.raw_path(entity_id, as_of_date, dv.id, file.filename)
    minio.upload_bytes(content, raw_p, content_type="application/octet-stream")

    dv.raw_path = raw_p
    db.commit()

    # Trigger async job
    task_normalize_and_dq.delay(str(dv.id))

    logger.info(f"Dataset upload queued | dv_id={dv.id} entity={entity_id} asof={as_of_date}")
    return {"dataset_version_id": dv.id, "status": "PENDING", "message": "Processing started"}


@router.get("", response_model=List[DatasetVersionOut])
def list_datasets(entity_id: Optional[str] = None, db: Session = Depends(get_db)):
    q = db.query(DatasetVersion)
    if entity_id:
        q = q.filter(DatasetVersion.entity_id == entity_id)
    dvs = q.order_by(DatasetVersion.created_at.desc()).all()
    return [_to_out(dv) for dv in dvs]


@router.get("/{dataset_version_id}", response_model=DatasetVersionOut)
def get_dataset(dataset_version_id: str, db: Session = Depends(get_db)):
    dv = db.query(DatasetVersion).filter(DatasetVersion.id == dataset_version_id).first()
    if not dv:
        raise HTTPException(404, "DatasetVersion not found")
    return _to_out(dv)


def _to_out(dv: DatasetVersion) -> DatasetVersionOut:
    return DatasetVersionOut(
        id=dv.id,
        entity_id=dv.entity_id,
        as_of_date=str(dv.as_of_date),
        source_name=dv.source_name,
        file_hash=dv.file_hash,
        status=dv.status,
        row_count=dv.row_count,
        total_balance=dv.total_balance,
        dq_summary=dv.dq_summary,
        created_at=str(dv.created_at),
    )
