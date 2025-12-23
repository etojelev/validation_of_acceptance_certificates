from datetime import date

from fastapi import HTTPException, status

from src.document.repository import DocumentsRepository
from src.document.schema import ValidatedOrder


class DocumentService:
    def __init__(self, repository: DocumentsRepository):
        self.repository = repository

    async def get_validated_order(
        self,
        begin_date: date | None,
        end_date: date | None,
        order_id: int | None,
        supply_id: str | None,
        account: str | None,
        page: int,
        page_size: int,
    ) -> list[ValidatedOrder]:
        if (
            begin_date
            and not isinstance(begin_date, date)
            or end_date
            and not isinstance(end_date, date)
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Некорректный формат даты. Введите дату в формате ГГГГ-ММ-ДД",
            )

        if begin_date and begin_date > date.today():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Начальная дата не может быть больше сегодняшнего дня.",
            )

        if supply_id and not isinstance(supply_id, str):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Некорректный формат ID поставки. Введите ID поставки в формате WB-GI-XXXXXXXXX.",
            )

        offset = (page - 1) * page_size

        records = await self.repository.get_validated_orders(
            begin_date=begin_date,
            end_date=end_date,
            order_id=order_id,
            supply_id=supply_id,
            account=account,
            page_size=page_size,
            offset=offset,
        )
        return [
            ValidatedOrder(
                order_id=record.get("order_id"),
                supply_id=record.get("supply_id"),
                sticker=record.get("sticker"),
                inner_order_status=record.get("status"),
                document=record.get("document"),
                account=record.get("account"),
                document_date=record.get("date"),
            )
            for record in records
        ]
