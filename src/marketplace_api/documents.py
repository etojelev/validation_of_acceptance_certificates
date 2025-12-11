from datetime import datetime, timedelta
from logging import getLogger
from typing import Any

from src.account import Account
from src.documents_validation.schema import DocumentSchema

logger = getLogger(__name__)


class Documents(Account):
    def __init__(self, account: str, token: str):
        super().__init__(account, token)
        self.base_url = "https://documents-api.wildberries.ru/api/v1/documents"

    async def _get_documents_by_fbs(self) -> list[DocumentSchema]:
        period_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        payload = {
            "locale": "ru",
            "beginTime": period_date,
            "endTime": period_date,
            "category": "act-income-mp",
        }
        async with self.async_client as session:
            response = await session.get(
                url=f"{self.base_url}/list", params=payload, headers=self.headers
            )

        return [
            DocumentSchema(
                act_income_name=document["serviceName"],
                supply_id=document["name"].split(" ")[-1].split(".")[0],
                document_created_at=datetime.strptime(
                    document["creationTime"], "%Y-%m-%dT%H:%M:%SZ"
                ),
            )
            for document in response["data"]["documents"]
        ]

    async def download_documents(self) -> dict[str, Any] | Any:
        documents = await self._get_documents_by_fbs()

        payload = {
            "params": [
                {"extension": "xlsx", "serviceName": document.act_income_name}
                for document in documents
            ]
        }

        async with self.async_client as session:
            response = await session.post(
                url=f"{self.base_url}/download/all", json=payload, headers=self.headers
            )
            return response
