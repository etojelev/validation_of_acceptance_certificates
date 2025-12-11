from typing import Any

from src.marketplace_api.documents import Documents
from src.response import AsyncHttpClient
from src.utils.utils import get_tokens


class DocumentsService:
    def __init__(self) -> None:
        self.async_client = AsyncHttpClient()

    async def download_documents(self) -> dict[str, Any] | Any:
        data = get_tokens()
        account = ""
        token = ""
        for acc, tok in data.items():
            account = acc
            token = tok
        documents_api = Documents(account=account, token=token)

        return await documents_api.download_documents()
