import asyncio
import base64
from datetime import date, datetime, timedelta
from logging import getLogger
from typing import Any

from asyncpg import Record

from src.celery.celery import celery_app
from src.dependencies.database import DatabasePoolManager
from src.documents_validation.repository import DocumentsRepository
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
            if isinstance(result, Exception):
                logger.error(f"Error for account {account}: {result}")
                documents_dict[account] = None
            else:
                documents_dict[account] = result

        return documents_dict

    async def extract_and_parce_excel(self) -> list:
        documents_dict = await self.download_documents()
        all_data = []

        for account, base64_string in documents_dict.items():
            if not base64_string:
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
            )
            for item in all_data
            for order_data in item["data"]
        ]
        return data_for_insert

    async def _sync_update_acceptance_certificates(self) -> None:
        fresh_data = await self.extract_and_parce_excel()

        if len(fresh_data) > 0:
            await self.documents_repository.update_acceptance_certificates(fresh_data)

    async def healthcheck(self) -> Record:
        return await self.documents_repository.get_data_for_healthcheck()


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

        document_service = DocumentsService(pool)

        logger.info("Проверка пополнения таблицы acceptance_fbs_acts_new")

        data = await document_service.healthcheck()
        if data:
            logger.info("Проверка данных успешно завершена!")

        if not data:
            logger.warning(
                f"Данных за {(datetime.now() - timedelta(days=1)).strftime('%d-%m-%Y')} не найдено! Попытка обновить данные!"
            )
            await document_service._sync_update_acceptance_certificates()
            logger.info("Данные успешно обновлены!")
    except Exception as error:
        logger.error(
            f"Ошибка в выполнении периодической задачи обновления актов приема передачи: {error}"
        )
    finally:
        if pool:
            await pool.close()
