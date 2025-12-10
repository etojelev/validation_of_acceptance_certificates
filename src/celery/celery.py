from logging import getLogger

from celery import Celery
from src.settings import get_settings

logger = getLogger(__name__)


def create_celery() -> Celery:
    celery_app = Celery(
        "validation-of-acceptance-certificate",
        broker=get_settings().CELERY_BROKER_URL,
        backend=get_settings().CELERY_RESULT_BACKEND,
    )

    return celery_app


celery_app = create_celery()


@celery_app.task(bind=True)  # type: ignore[untyped-decorator]
def debug_celery_task() -> str:
    logger.info("Debug celery task")
    return "Celery tasks debug successfully"
