from datetime import date, datetime

from pydantic import BaseModel, Field


class DocumentSchema(BaseModel):
    act_income_name: str
    supply_id: str
    document_created_at: datetime


class ValidatedOrder(BaseModel):
    order_id: int = Field(description="ID сборочного задания")
    supply_id: str | None = Field(
        description="ID поставки (None, если СЗ ещё не добавлена в поставку)"
    )
    sticker: int | None = Field(
        description="Стикер (None, если акта для данного СЗ нет)"
    )
    inner_order_status: str = Field(description="Внутренний статус")
    document: str | None = Field(
        description="Наименования акта приёма передачи (None, если акта ещё нет)"
    )
    account: str | None = Field(
        description="Наименование аккаунта (None, если акта ещё нет)"
    )
    document_date: date | None = Field(
        description="Дата создания акта приёма передачи (None, если СЗ ещё не добавлена в поставку)"
    )


class DocumentDataForValidate(BaseModel):
    account: str = Field(description="Имя аккаунта")
    document_number: str = Field(description="Номер акта приёма передачи")
    document_date: date = Field(
        description="Дата формирования акта приёма передачи в формате str"
    )
    supply_id: str = Field(description="ID поставки")
