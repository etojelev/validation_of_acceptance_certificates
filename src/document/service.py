import asyncio
from datetime import date, datetime, timedelta
from logging import getLogger

from fastapi import HTTPException, status

from src.document.repository import DocumentsRepository
from src.document.schema import (
    AcceptedOrdersWithoutCertificate,
    DocumentDataForValidate,
    ValidatedOrder,
    ValidateStatus,
)
from src.utils.utils import get_tokens

logger = getLogger(__name__)


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

    async def get_validated_orders_without_certificates(
        self,
    ) -> list[AcceptedOrdersWithoutCertificate]:
        result = await self.repository.get_accepted_orders_without_certificates()

        return [
            AcceptedOrdersWithoutCertificate(
                order_id=record.get("id"),
                supply_id=record.get("supply_id"),
                inner_status=record.get("inner_status"),
                supplier_status=record.get("supplier_status"),
                wb_status=record.get("wb_status"),
                reception_time=record.get("reception_time"),
                supply_name=record.get("supply_name"),
                account=record.get("account"),
            )
            for record in result
        ]

    async def get_document_number_and_supply_id(self) -> list[DocumentDataForValidate]:
        """
        Метод для получения ID поставок по дате формирования акта и имени аккаунта ДЛЯ ВСЕХ АККАУНТОВ
        """
        data = get_tokens()
        date_for_query = datetime.now() - timedelta(days=1)
        tasks = []
        for account in data.keys():
            task = self.repository.get_document_number_and_supply_id(
                document_date=date_for_query, account=account
            )
            tasks.append(task)

        records = await asyncio.gather(*tasks, return_exceptions=True)

        valid_records = []
        for account, result in zip(data.keys(), records, strict=False):
            if isinstance(result, Exception):
                logger.error(
                    f"Ошибка в получении данных из акта для аккаунта {account}: {result}"
                )
            elif isinstance(result, list):
                valid_records.extend(result)
            elif isinstance(result, dict):
                valid_records.append(result)

        return [
            DocumentDataForValidate(
                account=record.get("account"),
                document_number=record.get("document_number"),
                document_date=record.get("date"),
                supply_id=f"WB-GI-{record.get('document_number')}",
            )
            for record in valid_records
        ]

    async def get_validate_status(self) -> list[ValidateStatus]:
        tasks = []
        task_info = []
        result_list = []
        documents_data = await self.get_document_number_and_supply_id()
        for document in documents_data:
            task = self.repository.validate_orders(
                account=document.account,
                document_number=document.document_number,
                document_date=document.document_date,
                supply_id=document.supply_id,
            )
            tasks.append(task)
            task_info.append((document.account, document.document_number))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for (account, document_number), result in zip(task_info, results, strict=False):
            if isinstance(result, Exception):
                logger.error(
                    f"Аккаунт: {account}. Ошибка в валидации акта {document_number}: {result}"
                )
            elif isinstance(result, list):
                for record in result:
                    if record.get("sets_are_equal"):
                        logger.info(
                            f"Аккаунт: {account}, акт {document_number} валиден"
                        )
                        result_list.append(
                            ValidateStatus(
                                account=account,
                                document_number=document_number,
                                is_valid=record.get("sets_are_equal"),
                                matching_count=record.get("matching_count"),
                                only_in_our_service=None,
                                only_in_acts=None,
                            )
                        )
                    elif not record.get("sets_are_equal"):
                        logger.info(
                            f"Аккаунт: {account}, акт {document_number} РАСХОЖДЕНИЯ!\n"
                            f"Количество сборочных заданий в нашей базе данных: {record.get('only_in_our_service')}.\n"
                            f"Количество сборочных заданий в актах: {record.get('only_in_acts')}.\n"
                        )
                        result_list.append(
                            ValidateStatus(
                                account=account,
                                document_number=document_number,
                                is_valid=record.get("sets_are_equal"),
                                matching_count=record.get("matching_count"),
                                only_in_our_service=record.get("only_in_our_service"),
                                only_in_acts=record.get("only_in_acts"),
                            )
                        )
        return result_list
