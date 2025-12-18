from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from logging import getLogger
from typing import Any

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from src.dependencies.database import DatabasePoolManager
from src.handle_trigger.update_acceptance_certificates.router import update_certificates
from src.settings import get_settings

logger = getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    # создание экземпляра DatabasePoolManager
    database_pool_manager = DatabasePoolManager(
        user=get_settings().POSTGRES_USER,
        password=get_settings().POSTGRES_PASSWORD,
        db=get_settings().POSTGRES_DB,
        host=get_settings().POSTGRES_HOST,
        port=get_settings().POSTGRES_PORT,
        pool_size=get_settings().POOL_SIZE,
    )
    await database_pool_manager.create_pool()

    app.state.database_pool_manager = database_pool_manager

    logger.info("Приложение запущено, database pool manager создан")

    yield

    if hasattr(app.state, "database_pool_manager"):
        await app.state.database_pool_manager.close()
        logger.info("Database pool manager остановлен")


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

app.include_router(update_certificates)


@app.get("/")
async def root() -> dict[str, str]:
    return {"message": "Acceptance Certificates API is running!"}
