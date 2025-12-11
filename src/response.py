import asyncio
import json
from logging import getLogger
from typing import Any

import aiohttp

logger = getLogger(__name__)


class AsyncHttpClient:
    def __init__(
        self,
        timeout: int = 120,
        retries: int = 8,
        delay: int = 61,
        max_connections: int = 100,
    ):
        self.timeout = timeout
        self.retries = retries
        self.delay = delay
        self.max_connections = max_connections

        self._session: aiohttp.ClientSession | None = None
        self._session_owner = False

    async def __aenter__(self) -> Any:
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        await self.close()

    async def _ensure_session(self) -> None:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            connector = aiohttp.TCPConnector(
                limit=self.max_connections,
                force_close=False,
                enable_cleanup_closed=True,
            )
            self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
            self._session_owner = True

    async def close(self) -> None:
        if self._session_owner and self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            self._session_owner = False

    def _get_session(self) -> aiohttp.ClientSession | None:
        if self._session is not None and not self._session.closed:
            return self._session
        return None

    async def _make_request(self, method: str, url: str, **kwargs: Any) -> Any | None:
        if self._session is None or self._session.closed:
            await self._ensure_session()

        if self._session is None:
            logger.error("Не удалось создать сессию для HTTP-клиента")
            return None

        for attempt in range(self.retries):
            try:
                async with self._session.request(method, url, **kwargs) as response:
                    content_type = response.headers.get("Content-Type", "")
                    response.raise_for_status()
                    if content_type.startswith("image/"):
                        return await response.read()
                    return await response.json()
            except aiohttp.ClientConnectionError as error:
                logger.warning(
                    f"Попытка подключения {attempt + 1}: Ошибка во время {method} {url} - {error}"
                )
                if attempt < self.retries - 1:
                    await asyncio.sleep(self.delay)
                else:
                    if (
                        not self._session_owner
                        and self._session
                        and not self._session.closed
                    ):
                        await self._session.close()
                    return None
        return None

    async def request(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str | None:
        return await self._make_request(
            method, url, params=params, json=json, data=data, headers=headers
        )

    async def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str | None:
        return await self.request(method="GET", url=url, params=params, headers=headers)

    async def post(
        self,
        url: str,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str | None:
        return await self.request(
            method="POST", url=url, json=json, data=data, headers=headers
        )


def parse_json(text: str) -> dict[str, Any] | Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError(f"Ошибка парсинга JSON: {error}") from error
