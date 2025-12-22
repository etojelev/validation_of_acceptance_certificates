from logging import getLogger
from typing import Any

from asyncpg import (
    ConnectionDoesNotExistError,
    ConnectionFailureError,
    InterfaceError,
    PostgresError,
)

from src.dependencies.database import DatabasePoolManager
from src.utils.decorators import error_handler_http

logger = getLogger(__name__)


class DocumentsRepository:
    def __init__(self, database: DatabasePoolManager):
        self.database = database

    @error_handler_http(
        status_code=500,
        message="Database occure error",
        exceptions=(
            PostgresError,
            InterfaceError,
            ConnectionFailureError,
            ConnectionDoesNotExistError,
        ),
    )
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
