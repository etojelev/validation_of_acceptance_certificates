from fastapi import APIRouter, Depends

from src.celery.tasks.document_service import DocumentsService
from src.dependencies.handle_trigger.update_acceptance_certificates import (
    get_documents_validation_repository,
)

update_certificates = APIRouter(prefix="/handle_trigger", tags=["/handle_trigger"])


@update_certificates.post("/update_acceptance_certificates")
async def update_acceptance_certificates(
    documents_service: DocumentsService = Depends(get_documents_validation_repository),
) -> dict[str, str | int]:
    await documents_service._sync_update_acceptance_certificates()
    return {"status": 201, "message": "database updated"}
