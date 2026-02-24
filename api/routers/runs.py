"""N-BeMod — Runs router"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy.orm import Session

from db.session import get_db
from db.models import ScenarioRun, ScenarioDefinition, ModelVersion, ResultArtifact
from worker.tasks import task_run_cashflows
from storage import minio_client as minio

logger = logging.getLogger(__name__)
router = APIRouter()


class RunRequest(BaseModel):
    model_version_id: str
    scenario_name: str = "Base"   # Base | Shock+100 | Shock-100


class RunOut(BaseModel):
    id: str
    model_version_id: str
    scenario_definition_id: str
    status: str
    started_at: Optional[str]
    finished_at: Optional[str]
    error_message: Optional[str]
    created_at: str


class ArtifactOut(BaseModel):
    id: str
    artifact_type: str
    path: str
    summary_metrics_json: Optional[dict]
    created_at: str


SCENARIO_DEFAULTS = {
    "Base": {"rate_shock_bps": 0},
    "Shock+100": {"rate_shock_bps": 100},
    "Shock-100": {"rate_shock_bps": -100},
}


@router.post("", status_code=202)
def create_run(payload: RunRequest, db: Session = Depends(get_db)):
    mv = db.query(ModelVersion).filter(ModelVersion.id == payload.model_version_id).first()
    if not mv:
        raise HTTPException(404, "ModelVersion not found")
    if mv.status != "SUCCEEDED":
        raise HTTPException(422, f"ModelVersion status is {mv.status} — must be SUCCEEDED")

    scenario_def = payload.scenario_name
    if scenario_def not in SCENARIO_DEFAULTS:
        raise HTTPException(422, f"Unknown scenario: {scenario_def}. Use: {list(SCENARIO_DEFAULTS.keys())}")

    sd = db.query(ScenarioDefinition).filter(ScenarioDefinition.name == payload.scenario_name).first()
    if not sd:
        sd = ScenarioDefinition(
            name=payload.scenario_name,
            definition_json=SCENARIO_DEFAULTS[payload.scenario_name],
        )
        db.add(sd)
        db.commit()
        db.refresh(sd)

    run = ScenarioRun(model_version_id=mv.id, scenario_definition_id=sd.id, status="QUEUED")
    db.add(run)
    db.commit()
    db.refresh(run)

    task_run_cashflows.delay(str(run.id))
    logger.info(f"Run queued | run_id={run.id} | mv_id={mv.id} | scenario={payload.scenario_name}")
    return {"run_id": run.id, "status": "QUEUED"}


@router.get("/{run_id}", response_model=RunOut)
def get_run(run_id: str, db: Session = Depends(get_db)):
    run = db.query(ScenarioRun).filter(ScenarioRun.id == run_id).first()
    if not run:
        raise HTTPException(404, "Run not found")
    return _run_out(run)


@router.get("/{run_id}/artifacts", response_model=List[ArtifactOut])
def list_artifacts(run_id: str, db: Session = Depends(get_db)):
    artifacts = db.query(ResultArtifact).filter(ResultArtifact.scenario_run_id == run_id).all()
    return [_artifact_out(a) for a in artifacts]


@router.get("/artifacts/{artifact_id}/download")
def download_artifact(artifact_id: str, db: Session = Depends(get_db)):
    artifact = db.query(ResultArtifact).filter(ResultArtifact.id == artifact_id).first()
    if not artifact:
        raise HTTPException(404, "Artifact not found")
    data = minio.download_bytes(artifact.path)
    content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" \
        if artifact.path.endswith(".xlsx") else "application/octet-stream"
    filename = artifact.path.split("/")[-1]
    return Response(content=data, media_type=content_type,
                    headers={"Content-Disposition": f"attachment; filename={filename}"})


def _run_out(run: ScenarioRun) -> RunOut:
    return RunOut(
        id=run.id,
        model_version_id=run.model_version_id,
        scenario_definition_id=run.scenario_definition_id,
        status=run.status,
        started_at=str(run.started_at) if run.started_at else None,
        finished_at=str(run.finished_at) if run.finished_at else None,
        error_message=run.error_message,
        created_at=str(run.created_at),
    )


def _artifact_out(a: ResultArtifact) -> ArtifactOut:
    return ArtifactOut(
        id=a.id, artifact_type=a.artifact_type, path=a.path,
        summary_metrics_json=a.summary_metrics_json, created_at=str(a.created_at)
    )
