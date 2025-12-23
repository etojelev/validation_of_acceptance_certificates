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
        FROM (
            SELECT DISTINCT ON (order_id) *
            FROM order_status_log
            ORDER BY order_id, status, created_at DESC
        ) osl
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
    async def validate_orders(
        self, account: str, document_number: str, document_date: str, supply_id: str
    ) -> Record:
        query = """
        WITH orders_by_document_and_account AS (
            SELECT afan.order_number
            FROM acceptance_fbs_acts_new afan
            WHERE afan.account = $1
                AND afan.document_number = $2
                AND afan.date = $3
        ),
        orders_by_our_service AS (
            SELECT sao.id
            FROM supplies_and_orders sao
            WHERE sao.supply_id = $4
        ),
        matching_values AS (
            SELECT order_number AS value
            FROM orders_by_document_and_account
            INTERSECT
            SELECT id::text
            FROM orders_by_our_service
        ),
        only_in_first AS (
            SELECT order_number AS value
            FROM orders_by_document_and_account
            EXCEPT
            SELECT id::text
            FROM orders_by_our_service
        ),
        only_in_second AS (
            SELECT id::text AS value
            FROM orders_by_our_service
            EXCEPT
            SELECT order_number
            FROM orders_by_document_and_account
        )
        SELECT
            (SELECT COUNT(*) FROM matching_values) AS matching_count,
            (SELECT COUNT(*) FROM only_in_first) AS only_in_first_count,
            (SELECT COUNT(*) FROM only_in_second) AS only_in_second_count,
            CASE
                WHEN (SELECT COUNT(*) FROM only_in_first) = 0
                    AND (SELECT COUNT(*) FROM only_in_second) = 0
                THEN true
                ELSE false
            END AS sets_are_equal;
        """
        return await self.database.fetch(
            query, account, document_number, document_date, supply_id
        )
