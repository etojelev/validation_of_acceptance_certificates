from datetime import datetime

from pydantic import BaseModel


class DocumentSchema(BaseModel):
    act_income_name: str
    supply_id: str
    document_created_at: datetime
