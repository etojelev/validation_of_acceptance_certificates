import asyncio
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any

from fastapi import HTTPException

logger = logging.getLogger(__name__)


def error_handler(
    exceptions: tuple = (Exception,), default_return: Any = None
) -> Callable:
    """
    Декоратор для отлавливания и обработки исключений.
    :param exceptions: Котеж обрабатываемых исключений. По умолчанию: Exception
    :param default_return: Возвращаемое значение при обработке исключения. По умолчанию: None
    """

    def decorator(func: Callable) -> Callable:
        # проверка, является ли декорируемая функция корутиной
        is_async = asyncio.iscoroutinefunction(func)

        if is_async:

            @wraps(func)
            # async wrapper
            async def async_wrapper(*args: Any, **kwargs: Any) -> Callable | Any:
                try:
                    return await async_wrapper(*args, **kwargs)
                except exceptions as error:
                    logger.error("Error in %s: %s", func.__name__, error)
                    return default_return

            return async_wrapper

        else:

            @wraps(func)
            # sync wrapper
            def wrapper(*args: Any, **kwargs: Any) -> Callable | Any:
                try:
                    return func(*args, **kwargs)
                except exceptions as error:
                    logger.error("Error in %s: %s", func.__name__, error)
                    return default_return

            return wrapper

    return decorator


def error_handler_http(
    status_code: int,
    message: str = "Internal server error",
    exceptions: tuple = (Exception,),
) -> Callable:
    """Декоратор для поднятия HTTPException при обнаружении исключения.

    :pparam status_code: возвращаемый HTTP статус код при исключении.
    :param message: возвращаемое сообщение при исключении.
    :param exceptions: кортеж обрабатываемых исключений.
    """

    def decorator(func: Callable) -> Callable | Any:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Callable | Any:
            try:
                return await func(*args, **kwargs)
            except exceptions as error:
                logger.error("Ошибка в %s: %s", func.__name__, error)
                raise HTTPException(status_code=status_code, detail=message) from error

        return wrapper

    return decorator
