from datetime import datetime

from pydantic import BaseModel, Field


class DocumentSchema(BaseModel):
    act_income_name: str
    supply_id: str
    document_created_at: datetime


class HealthcheckStatusSchema(BaseModel):
    healthcheck_time: str = Field(description="Время проверки")
    is_healthcheck: bool = Field(description="Статус проверки")
    is_parcer_error: bool = Field(
        description="Статус исправности внутреннего парсера документов"
    )
    is_wb_api_error: bool = Field(description="Статус исправности WB API")


class HealthcheckStatusResponseModel(BaseModel):
    status: int
    data: list[HealthcheckStatusSchema]
