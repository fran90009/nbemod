"""
N-BeMod — SQLAlchemy ORM Models
"""
import uuid
from datetime import datetime
from sqlalchemy import (
    Column, String, Float, Integer, DateTime, Date, Text,
    ForeignKey, Enum as SAEnum, JSON
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


def gen_uuid():
    return str(uuid.uuid4())


class Entity(Base):
    __tablename__ = "entities"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    dataset_versions = relationship("DatasetVersion", back_populates="entity")


class DatasetVersion(Base):
    __tablename__ = "dataset_versions"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    entity_id = Column(UUID(as_uuid=False), ForeignKey("entities.id"), nullable=False)
    as_of_date = Column(Date, nullable=False)
    source_name = Column(String(512), nullable=False)
    file_hash = Column(String(64), nullable=False)
    status = Column(SAEnum("PENDING", "PROCESSING", "OK", "WARN", "KO", name="dv_status"), default="PENDING")

    raw_path = Column(Text, nullable=True)
    curated_path = Column(Text, nullable=True)
    dq_report_path = Column(Text, nullable=True)

    row_count = Column(Integer, nullable=True)
    total_balance = Column(Float, nullable=True)
    dq_summary = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    entity = relationship("Entity", back_populates="dataset_versions")
    model_versions = relationship("ModelVersion", back_populates="dataset_version")


class ModelDefinition(Base):
    __tablename__ = "model_definitions"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    name = Column(String(128), nullable=False)          # e.g. "prepay_curve"
    version = Column(String(32), nullable=False)         # e.g. "1.0"
    description = Column(Text, nullable=True)
    config_schema_json = Column(JSON, nullable=False)    # JSON Schema for params validation
    created_at = Column(DateTime, default=datetime.utcnow)

    model_versions = relationship("ModelVersion", back_populates="model_definition")


class ModelVersion(Base):
    __tablename__ = "model_versions"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    model_definition_id = Column(UUID(as_uuid=False), ForeignKey("model_definitions.id"), nullable=False)
    dataset_version_id = Column(UUID(as_uuid=False), ForeignKey("dataset_versions.id"), nullable=False)

    params_json = Column(JSON, nullable=False)       # config used for calibration
    artifact_path = Column(JSON, nullable=True)      # dict of paths (params.json, curves.parquet…)
    curves_path = Column(Text, nullable=True)
    summary_metrics = Column(JSON, nullable=True)

    status = Column(SAEnum("QUEUED", "RUNNING", "SUCCEEDED", "FAILED", name="mv_status"), default="QUEUED")
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    model_definition = relationship("ModelDefinition", back_populates="model_versions")
    dataset_version = relationship("DatasetVersion", back_populates="model_versions")
    scenario_runs = relationship("ScenarioRun", back_populates="model_version")


class ScenarioDefinition(Base):
    __tablename__ = "scenario_definitions"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    name = Column(String(128), nullable=False)       # Base / Shock+100 / Shock-100
    description = Column(Text, nullable=True)
    definition_json = Column(JSON, nullable=False)   # shock parameters
    created_at = Column(DateTime, default=datetime.utcnow)

    scenario_runs = relationship("ScenarioRun", back_populates="scenario_definition")


class ScenarioRun(Base):
    __tablename__ = "scenario_runs"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    model_version_id = Column(UUID(as_uuid=False), ForeignKey("model_versions.id"), nullable=False)
    scenario_definition_id = Column(UUID(as_uuid=False), ForeignKey("scenario_definitions.id"), nullable=False)

    status = Column(SAEnum("QUEUED", "RUNNING", "SUCCEEDED", "FAILED", name="run_status"), default="QUEUED")
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    model_version = relationship("ModelVersion", back_populates="scenario_runs")
    scenario_definition = relationship("ScenarioDefinition", back_populates="scenario_runs")
    result_artifacts = relationship("ResultArtifact", back_populates="scenario_run")


class ResultArtifact(Base):
    __tablename__ = "result_artifacts"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    scenario_run_id = Column(UUID(as_uuid=False), ForeignKey("scenario_runs.id"), nullable=False)

    artifact_type = Column(
        SAEnum("cashflows", "excel", "metrics", "plots", "curves", name="artifact_type"),
        nullable=False
    )
    path = Column(Text, nullable=False)
    summary_metrics_json = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    scenario_run = relationship("ScenarioRun", back_populates="result_artifacts")
