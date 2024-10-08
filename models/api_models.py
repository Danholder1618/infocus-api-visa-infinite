from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List

class Token(BaseModel):
    login: str
    password: str
    access_token: str
    refresh_token: str
    token_type: str
    expires_in: str
    refresh_expires_in: str

class AdditionalData(BaseModel):
    project_name: str
    project_value: int

class Customer(BaseModel):
    additional_phone: Optional[str]
    bank_manager_fio: Optional[str]
    bank_manager_phone: Optional[str]
    bank_product: Optional[str]
    bin: Optional[int]
    card_type_id: int
    clid: Optional[str]
    date_birth: datetime
    date_expiry: datetime
    email: Optional[str]
    firstname: str
    inn: Optional[str] = None
    language: str
    lastname: str
    manager: Optional[bool] = None
    manualSubscribe: Optional[bool] = None
    messageId: Optional[str] = None
    middlename: str
    pan: Optional[str] = None
    phone: str
    project_additional_data: Optional[List[AdditionalData]] = None
    service_level: str
    welcome: Optional[str] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }

class CustomerClose(BaseModel):
    id: int
    messageId: str = None
