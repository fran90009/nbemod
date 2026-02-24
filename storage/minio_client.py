"""
N-BeMod — MinIO / S3-compatible storage client
Convención de rutas:
  RAW:     entity=<id>/asof=<YYYY-MM-DD>/dataset_version=<id>/raw/<filename>
  CURATED: entity=<id>/asof=<YYYY-MM-DD>/dataset_version=<id>/curated/loans.parquet
  DQ:      entity=<id>/asof=<YYYY-MM-DD>/dataset_version=<id>/dq/dq_report.json
  MODEL:   entity=<id>/asof=<YYYY-MM-DD>/dataset_version=<id>/model_version=<id>/prepay_curve/params.json
  RUNS:    .../run_id=<id>/cashflows.parquet  |  .../run_id=<id>/export.xlsx
"""
import os
import io
import json
import logging
from typing import BinaryIO, Optional

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "nbemod_admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "nbemod_secret_minio")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "nbemod")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"


def get_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client: Optional[Minio] = None):
    c = client or get_client()
    if not c.bucket_exists(MINIO_BUCKET):
        c.make_bucket(MINIO_BUCKET)
        logger.info(f"Created bucket: {MINIO_BUCKET}")


# ─── Path helpers ─────────────────────────────────────────────────────────────

def raw_path(entity_id: str, asof: str, dv_id: str, filename: str) -> str:
    return f"entity={entity_id}/asof={asof}/dataset_version={dv_id}/raw/{filename}"

def curated_path(entity_id: str, asof: str, dv_id: str) -> str:
    return f"entity={entity_id}/asof={asof}/dataset_version={dv_id}/curated/loans.parquet"

def dq_report_path(entity_id: str, asof: str, dv_id: str) -> str:
    return f"entity={entity_id}/asof={asof}/dataset_version={dv_id}/dq/dq_report.json"

def model_params_path(entity_id: str, asof: str, dv_id: str, mv_id: str, model_name: str) -> str:
    return f"entity={entity_id}/asof={asof}/dataset_version={dv_id}/model_version={mv_id}/{model_name}/params.json"

def model_curves_path(entity_id: str, asof: str, dv_id: str, mv_id: str, model_name: str) -> str:
    return f"entity={entity_id}/asof={asof}/dataset_version={dv_id}/model_version={mv_id}/{model_name}/curves.parquet"

def run_cashflows_path(entity_id: str, asof: str, dv_id: str, mv_id: str, model_name: str, run_id: str) -> str:
    return f"entity={entity_id}/asof={asof}/dataset_version={dv_id}/model_version={mv_id}/{model_name}/run_id={run_id}/cashflows.parquet"

def run_excel_path(entity_id: str, asof: str, dv_id: str, mv_id: str, model_name: str, run_id: str) -> str:
    return f"entity={entity_id}/asof={asof}/dataset_version={dv_id}/model_version={mv_id}/{model_name}/run_id={run_id}/export.xlsx"


# ─── Upload / Download ────────────────────────────────────────────────────────

def upload_bytes(data: bytes, object_path: str, content_type: str = "application/octet-stream"):
    c = get_client()
    ensure_bucket(c)
    c.put_object(
        MINIO_BUCKET,
        object_path,
        io.BytesIO(data),
        length=len(data),
        content_type=content_type,
    )
    logger.info(f"Uploaded: {object_path} ({len(data)} bytes)")
    return object_path


def upload_file(local_path: str, object_path: str, content_type: str = "application/octet-stream"):
    c = get_client()
    ensure_bucket(c)
    c.fput_object(MINIO_BUCKET, object_path, local_path, content_type=content_type)
    logger.info(f"Uploaded file: {local_path} → {object_path}")
    return object_path


def download_bytes(object_path: str) -> bytes:
    c = get_client()
    response = c.get_object(MINIO_BUCKET, object_path)
    data = response.read()
    response.close()
    response.release_conn()
    return data


def upload_json(data: dict, object_path: str) -> str:
    raw = json.dumps(data, indent=2, default=str).encode("utf-8")
    return upload_bytes(raw, object_path, content_type="application/json")


def get_presigned_url(object_path: str, expires_hours: int = 1) -> str:
    from datetime import timedelta
    c = get_client()
    return c.presigned_get_object(MINIO_BUCKET, object_path, expires=timedelta(hours=expires_hours))
