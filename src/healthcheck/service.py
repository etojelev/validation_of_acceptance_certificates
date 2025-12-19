from asyncpg.protocol import Record
from fastapi import status

from src.documents_validation.schema import (
    HealthcheckStatusResponseModel,
    HealthcheckStatusSchema,
)
from src.healthcheck.repository import HealthcheckRepository


class HealthcheckService:
    def __init__(self, repository: HealthcheckRepository):
        self.repository = repository

    async def healthcheck(self) -> Record:
        return await self.repository.get_data_for_healthcheck()

    async def update_healthcheck_status(self, status_data: list) -> None:
        await self.repository.update_healthcheck_status(status_data=status_data)

    async def get_healthcheck_status(self) -> HealthcheckStatusResponseModel:
        healthcheck_statuses = await self.repository.get_healthcheck_status()

        data = [
            HealthcheckStatusSchema(
                healthcheck_time=status.get("healthcheck_time"),
                is_healthcheck=status.get("is_healthcheck_success"),
                is_parcer_error=status.get("is_parcer_error"),
                is_wb_api_error=status.get("is_wb_api_error"),
            )
            for status in healthcheck_statuses
        ]

        return HealthcheckStatusResponseModel(status=status.HTTP_200_OK, data=data)
