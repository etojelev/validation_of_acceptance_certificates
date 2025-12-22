from datetime import datetime
from logging import getLogger

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


class HealthcheckRepository:
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
    async def get_data_for_healthcheck(self) -> Record | None:
        query = """
        SELECT * FROM acceptance_fbs_acts_new
        WHERE created_at = CURRENT_DATE
        LIMIT 1;
        """
        return await self.database.fetchrow(query)

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
    async def update_healthcheck_status(
        self, status_data: list[datetime | bool]
    ) -> None:
        query = """
        INSERT INTO fbs_acts_healthcheck_status
        VALUES($1, $2, $3, $4);
        """

        await self.database.execute(query, *status_data)

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
    async def get_healthcheck_status(self) -> Record:
        query = """
        SELECT * FROM fbs_acts_healthcheck_status
        ORDER BY healthcheck_time DESC;
        """
        return await self.database.fetch(query)
