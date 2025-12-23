from typing import Any

from fastapi import Depends, Request

from src.dependencies.database import DatabasePoolManager
from src.document.repository import DocumentsRepository
from src.document.service import DocumentService


def get_database(request: Request) -> Any:
    return request.app.state.database_pool_manager


def get_validated_order_repository(
    database: DatabasePoolManager = Depends(get_database),
) -> DocumentsRepository:
    return DocumentsRepository(database=database)


def get_validated_order_service(
    repository: DocumentsRepository = Depends(get_validated_order_repository),
) -> DocumentService:
    return DocumentService(repository=repository)
