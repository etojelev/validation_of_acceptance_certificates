import asyncio
import base64
from logging import getLogger
from typing import Any

from src.marketplace_api.documents import Documents
from src.response import AsyncHttpClient
from src.utils.utils import extract_excel_from_zip, get_tokens

logger = getLogger(__name__)


class DocumentsService:
    def __init__(self) -> None:
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

    async def extract_and_parce_excel(self) -> list[list[dict[str, Any]]]:
        documents_dict = await self.download_documents()
        all_data = []

        for account, base64_string in documents_dict.items():
            if not base64_string:
                logger.warning(f"Аккаунт {account}: Нет данных для обработки!")
                continue

            try:
                zip_bytes = base64.b64decode(base64_string)
                account_data = extract_excel_from_zip(zip_bytes)

                for item in account_data:
                    item["account"] = account

                all_data.append(account_data)

                logger.info(
                    f"Аккаунт {account}: Обработано {len(account_data)} Excel файлов"
                )
            except Exception as error:
                logger.error(f"Аккаунт {account}: Ошибка обработки архива {error}")
        return all_data
