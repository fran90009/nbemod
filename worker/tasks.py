"""
N-BeMod — Celery Worker Tasks
Jobs: normalize_and_dq | calibrate_prepay_curve | run_cashflows
"""
import io
import logging
import os
from datetime import datetime

import pandas as pd
from celery import Celery

from db.session import db_session
from db.models import DatasetVersion, ModelVersion, ScenarioRun, ResultArtifact
from storage import minio_client as minio
from models.dq import run_dq_checks
from models.prepay_curve import calibrate_simple_average
from models.cashflows import compute_cashflows
from models.export import build_excel

logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
celery_app = Celery(
    "nbemod",
    broker=REDIS_URL,
    backend=os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")
)
celery_app.conf.task_serializer = "json"
celery_app.conf.result_serializer = "json"
celery_app.conf.accept_content = ["json"]


@celery_app.task(name="task_normalize_and_dq", bind=True, max_retries=2)
def task_normalize_and_dq(self, dataset_version_id: str):
    logger.info(f"[TASK normalize_and_dq] START | dv_id={dataset_version_id}")
    with db_session() as db:
        dv = db.query(DatasetVersion).filter(DatasetVersion.id == dataset_version_id).first()
        if not dv:
            return
        dv.status = "PROCESSING"

    try:
        with db_session() as db:
            dv = db.query(DatasetVersion).filter(DatasetVersion.id == dataset_version_id).first()
            entity_id = dv.entity_id
            asof = str(dv.as_of_date)

            raw_bytes = minio.download_bytes(dv.raw_path)
            fname = dv.source_name.lower()

            if fname.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(raw_bytes))
            elif fname.endswith((".xlsx", ".xls")):
                df = pd.read_excel(io.BytesIO(raw_bytes))
            else:
                raise ValueError(f"Unsupported file format: {fname}")

            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            col_map = {
                "as_of": "as_of_date", "asof": "as_of_date", "date": "as_of_date",
                "entity": "entity_id", "ent_id": "entity_id",
                "port": "portfolio", "loan_balance": "balance", "outstanding": "balance",
                "interest_rate": "rate", "coupon": "rate",
            }
            df.rename(columns=col_map, inplace=True)

            REQUIRED = ["as_of_date", "entity_id", "portfolio", "product", "balance"]
            for c in REQUIRED:
                if c not in df.columns:
                    df[c] = None

            if "segment" not in df.columns:
                df["segment"] = df.get("portfolio", "UNKNOWN").astype(str) + "_" + df.get("product", "UNKNOWN").astype(str)

            curated_p = minio.curated_path(str(entity_id), asof, dataset_version_id)
            minio.upload_bytes(df.to_parquet(index=False), curated_p)

            dq_report = run_dq_checks(df, dataset_version_id)
            dq_p = minio.dq_report_path(str(entity_id), asof, dataset_version_id)
            minio.upload_json(dq_report, dq_p)

            has_ko = any(c.get("result") == "KO" for c in dq_report.get("checks", []))
            has_warn = any(c.get("result") == "WARN" for c in dq_report.get("checks", []))
            status = "KO" if has_ko else ("WARN" if has_warn else "OK")

            dv.curated_path = curated_p
            dv.dq_report_path = dq_p
            dv.status = status
            dv.row_count = len(df)
            dv.total_balance = float(df["balance"].sum()) if "balance" in df.columns else None
            dv.dq_summary = {
                "status": status,
                "total_checks": len(dq_report.get("checks", [])),
                "ko_count": sum(1 for c in dq_report.get("checks", []) if c.get("result") == "KO"),
                "warn_count": sum(1 for c in dq_report.get("checks", []) if c.get("result") == "WARN")
            }

        logger.info(f"[TASK normalize_and_dq] DONE | dv_id={dataset_version_id} | status={status}")

    except Exception as e:
        logger.exception(f"[TASK normalize_and_dq] FAILED | dv_id={dataset_version_id}")
        with db_session() as db:
            dv = db.query(DatasetVersion).filter(DatasetVersion.id == dataset_version_id).first()
            if dv:
                dv.status = "KO"
                dv.dq_summary = {"error": str(e)}


@celery_app.task(name="task_calibrate_prepay_curve", bind=True, max_retries=2)
def task_calibrate_prepay_curve(self, model_version_id: str):
    logger.info(f"[TASK calibrate_prepay_curve] START | mv_id={model_version_id}")
    with db_session() as db:
        mv = db.query(ModelVersion).filter(ModelVersion.id == model_version_id).first()
        mv.status = "RUNNING"

    try:
        with db_session() as db:
            mv = db.query(ModelVersion).filter(ModelVersion.id == model_version_id).first()
            dv = mv.dataset_version
            entity_id = str(dv.entity_id)
            asof = str(dv.as_of_date)
            config = mv.params_json
            curated_path = dv.curated_path
            dv_id = str(dv.id)

            parquet_bytes = minio.download_bytes(curated_path)
            df = pd.read_parquet(io.BytesIO(parquet_bytes))

            curves_df, metrics = calibrate_simple_average(df, config, rate_shock_bps=0)

            curves_p = minio.model_curves_path(entity_id, asof, dv_id, model_version_id, "prepay_curve")
            minio.upload_bytes(curves_df.to_parquet(index=False), curves_p)

            params_p = minio.model_params_path(entity_id, asof, dv_id, model_version_id, "prepay_curve")
            minio.upload_json({**config, "metrics": metrics, "calibrated_at": str(datetime.utcnow())}, params_p)

            mv.status = "SUCCEEDED"
            mv.curves_path = curves_p
            mv.artifact_path = {"params": params_p, "curves": curves_p}
            mv.summary_metrics = metrics

        logger.info(f"[TASK calibrate_prepay_curve] DONE | mv_id={model_version_id}")

    except Exception as e:
        logger.exception(f"[TASK calibrate_prepay_curve] FAILED | mv_id={model_version_id}")
        with db_session() as db:
            mv = db.query(ModelVersion).filter(ModelVersion.id == model_version_id).first()
            if mv:
                mv.status = "FAILED"
                mv.error_message = str(e)


@celery_app.task(name="task_run_cashflows", bind=True, max_retries=2)
def task_run_cashflows(self, run_id: str):
    logger.info(f"[TASK run_cashflows] START | run_id={run_id}")
    with db_session() as db:
        run = db.query(ScenarioRun).filter(ScenarioRun.id == run_id).first()
        run.status = "RUNNING"
        run.started_at = datetime.utcnow()

    try:
        # Extraer todos los datos necesarios dentro de la sesión
        with db_session() as db:
            run = db.query(ScenarioRun).filter(ScenarioRun.id == run_id).first()
            mv = run.model_version
            dv = mv.dataset_version
            scenario = run.scenario_definition

            # Extraer valores primitivos antes de cerrar la sesión
            entity_id = str(dv.entity_id)
            asof = str(dv.as_of_date)
            config = dict(mv.params_json)
            curated_path = str(dv.curated_path)
            mv_id = str(mv.id)
            dv_id = str(dv.id)
            scenario_name = str(scenario.name)
            rate_shock_bps = int(scenario.definition_json.get("rate_shock_bps", 0))

        logger.info(f"Scenario: {scenario_name} | rate_shock_bps={rate_shock_bps}")

        # Procesar fuera de la sesión DB
        loan_bytes = minio.download_bytes(curated_path)
        loans_df = pd.read_parquet(io.BytesIO(loan_bytes))

        curves_df, scenario_metrics = calibrate_simple_average(
            loans_df, config, rate_shock_bps=rate_shock_bps
        )

        cashflows_df = compute_cashflows(loans_df, curves_df, config)

        cf_path = minio.run_cashflows_path(entity_id, asof, dv_id, mv_id, "prepay_curve", run_id)
        minio.upload_bytes(cashflows_df.to_parquet(index=False), cf_path)

        excel_path = minio.run_excel_path(entity_id, asof, dv_id, mv_id, "prepay_curve", run_id)
        excel_bytes = build_excel(curves_df, cashflows_df, {
            **config,
            "scenario": scenario_name,
            "rate_shock_bps": rate_shock_bps,
        }, run_id)
        minio.upload_bytes(
            excel_bytes, excel_path,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        summary = {
            "scenario": scenario_name,
            "rate_shock_bps": rate_shock_bps,
            "total_prepayment": float(cashflows_df["prepayment"].sum()),
            "avg_cpr": float(curves_df["cpr"].mean()),
            "segments": int(curves_df["segment"].nunique()),
            "months": int(cashflows_df["month"].max()),
        }

        # Guardar resultados en nueva sesión
        with db_session() as db:
            for artifact_type, path in [("cashflows", cf_path), ("excel", excel_path)]:
                ra = ResultArtifact(
                    scenario_run_id=run_id,
                    artifact_type=artifact_type,
                    path=path,
                    summary_metrics_json=summary,
                )
                db.add(ra)

            run = db.query(ScenarioRun).filter(ScenarioRun.id == run_id).first()
            run.status = "SUCCEEDED"
            run.finished_at = datetime.utcnow()

        logger.info(f"[TASK run_cashflows] DONE | run_id={run_id} | scenario={scenario_name} | shock={rate_shock_bps}bps")

    except Exception as e:
        logger.exception(f"[TASK run_cashflows] FAILED | run_id={run_id}")
        with db_session() as db:
            run = db.query(ScenarioRun).filter(ScenarioRun.id == run_id).first()
            if run:
                run.status = "FAILED"
                run.finished_at = datetime.utcnow()
                run.error_message = str(e)