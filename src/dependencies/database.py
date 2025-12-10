from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from src.settings import get_settings
from src.utils.decorators import error_handler


class DatabasePoolManager:
    """Класс для управления пулом соединений."""

    def __init__(
        self, user: str, password: str, db: str, host: str, port: int, pool_size: int
    ) -> None:
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = db
        self.pool_size = pool_size
        self.dsn = f"postgres://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}"

    async def create_pool(self) -> asyncpg.pool.Pool | None:
        """Создание пула соединений."""
        self.pool = asyncpg.pool.create_pool(
            dsn=self.dsn, min_size=self.pool_size, max_size=self.pool_size + 15
        )
        return self.pool

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Получение соединения из пула."""
        if not self.pool:
            await self.create_pool()

        async with self.pool.acquire() as connection:
            yield connection

    async def fetch(
        self, query: str, *args: Any, **kwargs: Any
    ) -> asyncpg.protocol.Record:
        """Выполнение запроса с возвратом множества значений."""
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args, **kwargs)

    async def fetchrow(
        self, query: str, *args: Any, **kwargs: Any
    ) -> asyncpg.protocol.Record:
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, *args, **kwargs)

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args, **kwargs)


database_pool_manager = DatabasePoolManager(
    user=get_settings().POSTGRES_USER,
    password=get_settings().POSTGRES_PASSWORD,
    db=get_settings().POSTGRES_DB,
    host=get_settings().POSTGRES_HOST,
    port=get_settings().POSTGRES_PORT,
    pool_size=get_settings().POOL_SIZE,
)

celery_pool_manager = DatabasePoolManager(
    user=get_settings().POSTGRES_USER,
    password=get_settings().POSTGRES_PASSWORD,
    db=get_settings().POSTGRES_DB,
    host=get_settings().POSTGRES_HOST,
    port=get_settings().POSTGRES_PORT,
    pool_size=get_settings().POOL_SIZE,
)


@error_handler()
async def check_pool_created() -> None:
    if not database_pool_manager.pool:
        await database_pool_manager.create_pool()
    await database_pool_manager.execute("SELECT 1")

    if not celery_pool_manager.pool:
        await celery_pool_manager.create_pool()


@error_handler()
async def check_pool_stopped() -> None:
    if database_pool_manager.pool:
        await database_pool_manager.pool.close()

    if celery_pool_manager.pool:
        await celery_pool_manager.pool.close()
