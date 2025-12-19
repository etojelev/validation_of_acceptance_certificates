from typing import Any

from fastapi import Depends, Request

from src.dependencies.database import DatabasePoolManager
from src.healthcheck.repository import HealthcheckRepository
from src.healthcheck.service import HealthcheckService


def get_database(request: Request) -> Any:
    return request.app.state.database_pool_manager


def get_healthcheck_repository(
    database: DatabasePoolManager = Depends(get_database),
) -> HealthcheckRepository:
    return HealthcheckRepository(database=database)


def get_healthcheck_service(
    repository: HealthcheckRepository = Depends(get_healthcheck_repository),
) -> HealthcheckService:
    return HealthcheckService(repository=repository)
