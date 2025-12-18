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
        self.pool: asyncpg.pool.Pool | None = None

    async def create_pool(self) -> asyncpg.pool.Pool:
        """Создание пула соединений."""
        if self.pool is None:
            self.pool = await asyncpg.pool.create_pool(
                dsn=self.dsn, min_size=self.pool_size, max_size=self.pool_size + 15
            )
        return self.pool

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Получение соединения из пула."""
        if not self.pool is not None:
            await self.create_pool()
        assert self.pool is not None
        async with self.pool.acquire() as connection:
            yield connection

    async def fetch(
        self, query: str, *args: Any, **kwargs: Any
    ) -> asyncpg.protocol.Record:
        """Выполнение запроса с возвратом множества значений."""
        assert self.pool is not None
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args, **kwargs)

    async def fetchrow(
        self, query: str, *args: Any, **kwargs: Any
    ) -> asyncpg.protocol.Record:
        assert self.pool is not None
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, *args, **kwargs)

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        assert self.pool is not None
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args, **kwargs)

    async def executemany(self, query: str, *args: Any, **kwargs: Any) -> Any:
        assert self.pool is not None
        async with self.pool.acquire() as connection:
            return await connection.executemany(query, *args, **kwargs)


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
