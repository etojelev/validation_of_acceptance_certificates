from logging import getLogger

from celery.schedules import crontab

from celery import Celery
from src.settings import get_settings

logger = getLogger(__name__)


def create_celery() -> Celery:
    celery_app = Celery(
        "validation-of-acceptance-certificate",
        broker=get_settings().CELERY_BROKER_URL,
        backend=get_settings().CELERY_RESULT_BACKEND,
    )

    celery_app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="Europe/Moscow",
        enable_utc=True,
    )

    celery_app.conf.beat_schedule = {
        "update-certificates": {
            "task": "update_acceptance_certificates_task",
            "schedule": crontab(hour=4, minute=10),
        },
        "healthcheck": {"task": "healthcheck", "schedule": crontab(hour=6, minute=30)},
        "validate_orders": {
            "task": "validate_orders",
            "schedule": crontab(hour=8),
        },
    }

    return celery_app


celery_app = create_celery()

celery_app.autodiscover_tasks(
    [
        "src.celery.tasks.document_service",
    ]
)


@celery_app.task(bind=True)
def debug_celery_task() -> str:
    logger.info("Debug celery task")
    return "Celery tasks debug successfully"
