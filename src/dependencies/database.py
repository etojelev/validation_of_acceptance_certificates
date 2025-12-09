from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg


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
