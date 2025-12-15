import asyncio
from logging import getLogger
from typing import Any

from src.marketplace_api.documents import Documents
from src.response import AsyncHttpClient
from src.utils.utils import get_tokens

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
            *(task for _, task in tasks),
            return_exceptions=True
        )

        documents_list = []
        for (account, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                logger.error(f"Error for account {account}: {result}")
                documents_list.append({account: None})
            else:
                documents_list.append({account: result})

        return documents_list
