from typing import Any

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware


def add_middleware(app: FastAPI, *args: Any, **kwargs: Any) -> None:
    app.add_middleware(*args, **kwargs)


def start_application() -> FastAPI:
    application = FastAPI(title="Validation Acceptance Certificates")
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
