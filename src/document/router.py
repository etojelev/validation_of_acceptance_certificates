from datetime import date

from fastapi import APIRouter, Depends, Query, status

from src.dependencies.get_validated_order import get_validated_order_service
from src.document.schema import (
    AcceptedOrdersWithoutCertificate,
    ValidatedOrder,
    ValidateStatus,
)
from src.document.service import DocumentService

validated_order = APIRouter(prefix="/validated_order", tags=["/validated_order"])


@validated_order.get(
    "/", response_model=list[ValidatedOrder], status_code=status.HTTP_200_OK
)
async def get_validated_order(
    begin_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    order_id: int | None = Query(default=None),
    supply_id: str | None = Query(default=None),
    account: str | None = Query(default=None),
    page: int = Query(1, ge=1),
    page_size: int = Query(200, ge=1),
    service: DocumentService = Depends(get_validated_order_service),
) -> list[ValidatedOrder]:
    return await service.get_validated_order(
        begin_date=begin_date,
        end_date=end_date,
        order_id=order_id,
        supply_id=supply_id,
        account=account,
        page=page,
        page_size=page_size,
    )


@validated_order.get(
    "/not_confirmed",
    response_model=list[AcceptedOrdersWithoutCertificate],
    status_code=status.HTTP_200_OK,
)
async def get_accepted_orders_without_certificates(
    service: DocumentService = Depends(get_validated_order_service),
) -> list[AcceptedOrdersWithoutCertificate]:
    return await service.get_validated_orders_without_certificates()


@validated_order.get(
    "/status", response_model=list[ValidateStatus], status_code=status.HTTP_200_OK
)
async def get_validate_status(
    service: DocumentService = Depends(get_validated_order_service),
) -> list[ValidateStatus]:
    return await service.get_validate_status()
