"""N-BeMod — Entities router"""
import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional, List
from db.session import get_db
from db.models import Entity

logger = logging.getLogger(__name__)
router = APIRouter()


class EntityCreate(BaseModel):
    name: str
    description: Optional[str] = None


class EntityOut(BaseModel):
    id: str
    name: str
    description: Optional[str]
    created_at: str

    class Config:
        from_attributes = True


@router.post("", response_model=EntityOut, status_code=201)
def create_entity(payload: EntityCreate, db: Session = Depends(get_db)):
    existing = db.query(Entity).filter(Entity.name == payload.name).first()
    if existing:
        raise HTTPException(status_code=409, detail=f"Entity '{payload.name}' already exists")
    entity = Entity(name=payload.name, description=payload.description)
    db.add(entity)
    db.commit()
    db.refresh(entity)
    logger.info(f"Created entity: {entity.id} | {entity.name}")
    return EntityOut(id=entity.id, name=entity.name, description=entity.description, created_at=str(entity.created_at))


@router.get("", response_model=List[EntityOut])
def list_entities(db: Session = Depends(get_db)):
    entities = db.query(Entity).order_by(Entity.created_at.desc()).all()
    return [EntityOut(id=e.id, name=e.name, description=e.description, created_at=str(e.created_at)) for e in entities]
