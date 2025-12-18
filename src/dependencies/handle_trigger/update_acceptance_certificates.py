from typing import Any

from fastapi import Depends, Request

from src.celery.tasks.document_service import DocumentsService
from src.dependencies.database import DatabasePoolManager


def get_database(request: Request) -> Any:
    return request.app.state.database_pool_manager


def get_documents_validation_repository(
    database: DatabasePoolManager = Depends(get_database),
) -> DocumentsService:
    return DocumentsService(db=database)
