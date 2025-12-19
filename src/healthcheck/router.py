from fastapi import APIRouter, Depends

from src.dependencies.get_healthcheck_status import get_healthcheck_service
from src.documents_validation.schema import HealthcheckStatusResponseModel
from src.healthcheck.service import HealthcheckService

healthcheck = APIRouter(prefix="/healthcheck", tags=["/healthxheck"])


@healthcheck.get("/status")
async def get_healthcheck_status(
    healthcheck_service: HealthcheckService = Depends(get_healthcheck_service),
) -> HealthcheckStatusResponseModel:
    return await healthcheck_service.get_healthcheck_status()
