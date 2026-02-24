"""N-BeMod — Health router"""
from fastapi import APIRouter
router = APIRouter()

@router.get("/health")
def health():
    return {"status": "ok", "service": "N-BeMod API", "version": "0.1.0"}
