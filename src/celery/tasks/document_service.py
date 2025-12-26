import asyncio
import base64
from datetime import date, datetime, timedelta
from logging import getLogger
from typing import Any

from src.celery.celery import celery_app
from src.dependencies.database import DatabasePoolManager
from src.document.repository import DocumentsRepository
from src.document.schema import DocumentDataForValidate
from src.healthcheck.schema import HealthcheckStatus
from src.healthcheck.service import HealthcheckRepository, HealthcheckService
from src.marketplace_api.documents import Documents
from src.response import AsyncHttpClient
from src.utils.utils import extract_excel_from_zip, get_tokens

logger = getLogger(__name__)


class DocumentsService:
    def __init__(self, db: DatabasePoolManager) -> None:
        self.db = db
        self.documents_repository = DocumentsRepository(db)
        self.async_client = AsyncHttpClient()

    async def download_documents(self) -> dict[str, Any] | Any:
        data = get_tokens()

        tasks = []
        for account, token in data.items():
            documents_api = Documents(account=account, token=token)
            task = documents_api.download_documents()
            tasks.append((account, task))

        results = await asyncio.gather(
            *(task for _, task in tasks), return_exceptions=True
        )

        documents_dict: dict[str, Any] = {}
        for (account, _), result in zip(tasks, results, strict=False):
            if isinstance(result, Exception) or not isinstance(result, str):
                logger.error(f"Error for account {account}: {result}")
                documents_dict[account] = None
            else:
                documents_dict[account] = result

        return documents_dict

    async def extract_and_parce_excel(self) -> list | int:
        documents_dict = await self.download_documents()
        all_data = []
        update_date = date.today()

        for account, base64_string in documents_dict.items():
            if base64_string is None:
                logger.warning(f"Аккаунт {account}: Нет данных для обработки!")
                continue

            try:
                zip_bytes = base64.b64decode(base64_string)
                account_data_list = extract_excel_from_zip(zip_bytes)

                for item in account_data_list:
                    item["account"] = account
                    all_data.append(item)

                logger.info(
                    f"Аккаунт {account}: Обработано {len(account_data_list)} Excel файлов"
                )
            except Exception as error:
                logger.error(f"Аккаунт {account}: Ошибка обработки архива {error}")

        data_for_insert = [
            (
                str(order_data["order_id"]),
                str(order_data["sticker"]),
                int(order_data["count"]),
                f"act-income-mp-{item['supply_id'].split('-')[-1]}.zip",
                item["supply_id"].split("-")[-1],
                date.fromisoformat(item["date"]),
                item["account"],
                update_date,
            )
            for item in all_data
            for order_data in item["data"]
        ]
        return data_for_insert

    async def _sync_update_acceptance_certificates(self) -> None | int:
        fresh_data = await self.extract_and_parce_excel()

        if isinstance(fresh_data, list) and len(fresh_data) > 0:
            await self.documents_repository.update_acceptance_certificates(fresh_data)
            return None
        if isinstance(fresh_data, int):
            return fresh_data
        return None

    async def get_document_number_and_supply_id(self) -> list[DocumentDataForValidate]:
        """
        Метод для получения ID поставок по дате формирования акта и имени аккаунта ДЛЯ ВСЕХ АККАУНТОВ
        """
        data = get_tokens()
        date_for_query = datetime.now() - timedelta(days=1)
        tasks = []
        for account in data.keys():
            task = self.documents_repository.get_document_number_and_supply_id(
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

    async def validate_orders(
        self, documents_data: list[DocumentDataForValidate]
    ) -> None:
        logger.info("Выполнение валидации акта приёма передачи")
        tasks = []
        task_info = []
        for document in documents_data:
            task = self.documents_repository.validate_orders(
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
                    elif not record.get("sets_are_equal"):
                        logger.info(
                            f"Аккаунт: {account}, акт {document_number} РАСХОЖДЕНИЯ!\n"
                            f"Количество сборочных заданий в нашей базе данных: {record.get('only_in_our_service')}.\n"
                            f"Количество сборочных заданий в актах: {record.get('only_in_acts')}.\n"
                        )


@celery_app.task(name="update_acceptance_certificates_task")
def auto_update_acceptance_certificates() -> None:
    try:
        logger.info("Выполнение периодической задачи обновления актов приема передачи")
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(_update_acceptance_certificates_async())
    except Exception as error:
        logger.error(
            f"Ошибка в выполнении периодической задачи обновления актов приема передачи: {error}"
        )


async def _update_acceptance_certificates_async() -> None:
    from src.settings import get_settings

    pool = None
    try:
        pool = DatabasePoolManager(
            user=get_settings().POSTGRES_USER,
            password=get_settings().POSTGRES_PASSWORD,
            db=get_settings().POSTGRES_DB,
            host=get_settings().POSTGRES_HOST,
            port=get_settings().POSTGRES_PORT,
            pool_size=get_settings().POOL_SIZE,
        )

        await pool.create_pool()

        document_service = DocumentsService(pool)
        await document_service._sync_update_acceptance_certificates()
    except Exception as error:
        logger.error(
            f"Ошибка в выполнении периодической задачи обновления актов приема передачи: {error}"
        )
    finally:
        if pool:
            await pool.close()


@celery_app.task(name="healthcheck")
def auto_healthcheck() -> None:
    try:
        logger.info("Выполнение healthcheck")
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(_healthcheck())
    except Exception as error:
        logger.error(f"Ошибка в выполнении healthcheck: {error}")


async def _healthcheck() -> None:
    from src.settings import get_settings

    pool = None
    try:
        pool = DatabasePoolManager(
            user=get_settings().POSTGRES_USER,
            password=get_settings().POSTGRES_PASSWORD,
            db=get_settings().POSTGRES_DB,
            host=get_settings().POSTGRES_HOST,
            port=get_settings().POSTGRES_PORT,
            pool_size=get_settings().POOL_SIZE,
        )

        await pool.create_pool()

        healthcheck_repository = HealthcheckRepository(database=pool)
        healthcheck_service = HealthcheckService(repository=healthcheck_repository)
        document_service = DocumentsService(pool)

        logger.info("Проверка пополнения таблицы acceptance_fbs_acts_new")

        data = await healthcheck_service.healthcheck()
        if data:
            logger.info("Проверка данных успешно завершена!")
            status_data = HealthcheckStatus.SUCCESS.result

        if not data:
            logger.warning(
                f"Данных за {(datetime.now() - timedelta(days=1)).strftime('%d-%m-%Y')} не найдено! Попытка обновить данные!"
            )
            try:
                another_try = (
                    await document_service._sync_update_acceptance_certificates()
                )
                if another_try is not None:
                    logger.warning(
                        f"WB API статус: {another_try}! WB API работает не стабильно! Данные не целостны, запись в БД не будет произведена!"
                    )
                    status_data = HealthcheckStatus.WB_API_FAIL.result
                if another_try is None:
                    logger.info("Данные успешно обновлены!")
                    status_data = HealthcheckStatus.SUCCESS.result
            except Exception as error:
                logger.error(
                    f"Ошибка в работе сервиса: {error}! Данные не целостны, запись в БД не будет произведена!"
                )
                status_data = HealthcheckStatus.INNER_METHOD_FAIL.result

        await healthcheck_service.update_healthcheck_status(status_data=status_data)

    except Exception as error:
        logger.error(
            f"Ошибка в выполнении периодической задачи обновления актов приема передачи: {error}"
        )
    finally:
        if pool:
            await pool.close()


@celery_app.task(name="validate_orders")
def auto_validate_orders() -> None:
    try:
        logger.info("Выполнение автоматической валидации актов приёма передачи")
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(_validate_orders())
    except Exception as error:
        logger.error(
            f"Ошибка в выполнении автоматической валидации актов приёма передачи: {error}"
        )


async def _validate_orders() -> None:
    from src.settings import get_settings

    pool = None
    try:
        pool = DatabasePoolManager(
            user=get_settings().POSTGRES_USER,
            password=get_settings().POSTGRES_PASSWORD,
            db=get_settings().POSTGRES_DB,
            host=get_settings().POSTGRES_HOST,
            port=get_settings().POSTGRES_PORT,
            pool_size=get_settings().POOL_SIZE,
        )

        await pool.create_pool()

        document_service = DocumentsService(db=pool)
        document_data = await document_service.get_document_number_and_supply_id()
        await document_service.validate_orders(documents_data=document_data)
    except Exception as error:
        logger.error(
            f"Ошибка в выполнении периодической задачи валидации актов приема передачи: {error}"
        )
    finally:
        if pool:
            await pool.close()
