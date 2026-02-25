"""
N-BeMod — FastAPI Application Entry Point
"""
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import health, entities, datasets, models, runs, backtesting

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

app = FastAPI(
    title="N-BeMod API",
    description="Behavioural Model Calibration Platform — ALM / IRRBB / Liquidity",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, tags=["Health"])
app.include_router(entities.router, prefix="/entities", tags=["Entities"])
app.include_router(datasets.router, prefix="/datasets", tags=["Datasets"])
app.include_router(models.router, prefix="/models", tags=["Models"])
app.include_router(runs.router, prefix="/runs", tags=["Runs"])
app.include_router(backtesting.router, prefix="/backtesting", tags=["Backtesting"])
