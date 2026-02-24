import os
from celery import Celery

REDIS_URL = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0")

celery_app = Celery(
    "nbemod",
    broker=REDIS_URL,
    backend=os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379/1"),
    include=["worker.tasks"]
)

celery_app.conf.task_serializer = "json"
celery_app.conf.result_serializer = "json"
celery_app.conf.accept_content = ["json"]
