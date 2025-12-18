from logging import getLogger
from typing import Any

from src.dependencies.database import DatabasePoolManager

logger = getLogger(__name__)


class DocumentsRepository:
    def __init__(self, database: DatabasePoolManager):
        self.database = database

    async def update_acceptance_certificates(
        self, certificates: list[set[Any]]
    ) -> None:
        query = """
        INSERT INTO acceptance_fbs_acts_new
            (order_number, unit, sticker, quantity, document, document_number, date, account)
        VALUES
            ($1, 'шт.', $2, $3, $4, $5, $6, $7)
        """

        await self.database.executemany(query, certificates)
