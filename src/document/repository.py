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
            (order_number, unit, sticker, quantity, document, document_number, date, account, created_at)
        VALUES
            ($1, 'шт.', $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT DO NOTHING
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
        WITH acts AS (
            SELECT afan.order_number::text AS order_id
            FROM acceptance_fbs_acts_new afan
            WHERE afan.account = $1
              AND afan.document_number = $2::text
              AND afan.date = $3
        ),
        our_service AS (
            SELECT id::text AS order_id
            FROM (
                SELECT
                    atsm.id,
                    atsm.supplier_status,
                    atsm.wb_status,
                    ROW_NUMBER() OVER (
                        PARTITION BY atsm.id
                        ORDER BY atsm.created_at_db DESC
                    ) AS rn
                FROM assembly_task_status_model atsm
                WHERE atsm.supply_id = $4
            ) t
            WHERE t.rn = 1
              AND
                (t.supplier_status <> 'cancel'
                    OR t.wb_status NOT IN (
                        'canceled',
                        'canceled_by_client',
                        'declined_by_client',
                        'defect'
                    )
                )
            )
        SELECT
            COUNT(*) FILTER (WHERE a.order_id IS NOT NULL AND o.order_id IS NOT NULL) AS matching_count,
            COUNT(*) FILTER (WHERE a.order_id IS NOT NULL AND o.order_id IS NULL)     AS only_in_acts,
            COUNT(*) FILTER (WHERE a.order_id IS NULL AND o.order_id IS NOT NULL)     AS only_in_our_service,
            COUNT(*) FILTER (WHERE a.order_id IS NULL OR o.order_id IS NULL) = 0      AS sets_are_equal
        FROM acts a
        FULL JOIN our_service o
               ON o.order_id = a.order_id;
        """
        return await self.database.fetch(
            query, account, document_number, document_date, supply_id
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
    async def get_document_number_and_supply_id(
        self, document_date: str, account: str
    ) -> Record:
        query = """
        SELECT DISTINCT
            afan.document_number,
            afan.account,
            afan.date
        FROM acceptance_fbs_acts_new afan
        WHERE afan.date = $1::date AND
        afan.account = $2::text;
        """

        return await self.database.fetch(query, document_date, account)
