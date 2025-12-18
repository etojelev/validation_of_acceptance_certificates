from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from src.dependencies.database import check_pool_created, check_pool_stopped


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    # выполнение проверки создания пулов соединений к бд ДО запуска приложения
    await check_pool_created()
    yield
    # выполнение проверки отключений пулов соединений к бд ПОСЛЕ запуска приложения
    await check_pool_stopped()


def add_middleware(app: FastAPI, *args: Any, **kwargs: Any) -> None:
    app.add_middleware(*args, **kwargs)


def start_application() -> FastAPI:
    application = FastAPI(title="Validation Acceptance Certificates", lifespan=lifespan)
    add_middleware(
        application,
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return application


app = start_application()
