from pydantic import BaseModel, EmailStr, constr
from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from .database import Base

class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, index=True)
    firstname = Column(String, index=True)
    lastname = Column(String, index=True)
    middlename = Column(String, index=True)
    phone = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    date_birth = Column(DateTime)
    date_expiry = Column(DateTime)
    inn = Column(String)
    bin = Column(String)
    card_type_id = Column(Integer)
    clid = Column(String)
    language = Column(String)
    service_level = Column(String)



class CustomerBase(BaseModel):
    additional_phone: Optional[str]
    bank_manager_fio: Optional[str]
    bank_manager_phone: Optional[str]
    bank_product: Optional[str]
    bin: Optional[str]
    card_type_id: int
    clid: Optional[str]
    date_birth: datetime
    date_expiry: datetime
    email: Optional[str]


    firstname: Optional[str] = None
    lastname: Optional[str] = None
    middlename: Optional[str] = None
    inn: Optional[str] = None
    clid: Optional[str] = None
    language: Optional[str] = None
    project_additional_data: Optional[dict] = None
    service_level: str
    welcome: Optional[str] = None

class CustomerCreate(CustomerBase):
    phone: constr(regex=r'^\+?[1-9]\d{1,14}$')
    email: EmailStr

class CustomerUpdate(CustomerBase):
    pass

class CustomerInDBBase(CustomerBase):
    id: int

    class Config:
        orm_mode = True

class Customer(CustomerInDBBase):
    pass
