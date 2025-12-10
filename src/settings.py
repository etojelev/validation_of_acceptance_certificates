from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    POSTGRES_USER: str = Field(default="postgres_user")
    POSTGRES_PASSWORD: str = Field(default="postgres_password")
    POSTGRES_DB: str = Field(default="postgres_db")
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    POOL_SIZE: int = Field(default=5)

    REDIS_HOST: str = Field(default="redis")
    REDIS_PORT: int = Field(default=6379)
    REDIS_DB: int = Field(default=0)
    REDIS_PASSWORD: str = Field(default="")
    CACHE_TTL: int = Field(default=3600)
    CACHE_REFRESH_INTERVAL: int = Field(default=3600)

    CELERY_BROKER_URL: str = Field(default="amqp://guest:guest@localhost")
    CELERY_RESULT_BACKEND: str = Field(default="amqp://guest:guest@localhost")
    CELERY_TIMEZONE: str = Field(default="UTC")
    CELERY_RESULT_EXPIRES: int = Field(default=3600)
    CELERY_WORKER_PREFETCH_MULTIPLIER: int = Field(default=1)
    CELERY_WORKER_MAX_TASKS_PER_CHILD: int = Field(default=10)
    CELERY_TASK_SOFT_TIME_LIMIT: int = Field(default=300)
    CELERY_TASK_TIME_LIMIT: int = Field(default=300)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


@lru_cache
def get_settings() -> Settings:
    return Settings()
