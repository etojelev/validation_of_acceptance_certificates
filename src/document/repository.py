from datetime import date
from logging import getLogger
from typing import Any

from asyncpg import (
    ConnectionDoesNotExistError,
    ConnectionFailureError,
    InterfaceError,
    PostgresError,
)
from asyncpg.protocol import Record

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
    async def get_validated_orders(
        self,
        begin_date: date,
        end_date: date,
        order_id: int,
        supply_id: str,
        account: str,
        page_size: int,
        offset: int,
    ) -> Record:
        query = """
        SELECT
            osl.order_id,
            osl.supply_id,
            afa.sticker,
            osl.status,
            afa.document,
            afa.account,
            afa.date
        FROM order_status_log osl
        LEFT JOIN acceptance_fbs_acts_new afa
        ON osl.order_id::text = afa.order_number
        WHERE
            ($1::date IS NULL OR afa.date >= $1::date) AND
            ($2::date IS NULL OR afa.date <= $2::date) AND
            ($3::int8 IS NULL OR osl.order_id = $3::int8) AND
            ($4::varchar IS NULL OR osl.supply_id = $4::varchar) AND
            ($5::varchar IS NULL or afa.account = $5::varchar)
        LIMIT $6
        OFFSET $7;
        """
        return await self.database.fetch(
            query, begin_date, end_date, order_id, supply_id, account, page_size, offset
        )
