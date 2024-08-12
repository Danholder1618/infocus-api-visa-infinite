from sqlalchemy import Column, Integer, String, DateTime, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Token(Base):
    __tablename__ = 'tokens'
    
    id = Column(Integer, primary_key=True, index=True)
    login=Column(String)
    password=Column(String)
    access_token = Column(String, unique=True, index=True)
    refresh_token = Column(String)
    token_type = Column(String)
    expires_in = Column(Integer)
    refresh_expires_in = Column(Integer)

class Customer(Base):
    __tablename__ = "infinite_customers"

    id = Column(Integer, primary_key=True, index=True)
    additional_phone = Column(String, unique=True, index=True)
    bank_manager_fio = Column(String, index=True)
    bank_manager_phone = Column(String, unique=True, index=True)
    bank_product = Column(String)
    bin = Column(Integer)
    card_type_id = Column(Integer)
    clid = Column(String)
    date_birth = Column(DateTime)
    date_expiry = Column(DateTime)
    email = Column(String, unique=True, index=True)
    firstname = Column(String, index=True)
    inn = Column(String)
    language = Column(String)
    lastname = Column(String, index=True)
    manager = Column(Boolean)
    manualSubscribe = Column(Boolean)
    messageId = Column(String)
    middlename = Column(String)
    pan = Column(String)
    phone = Column(String, unique=True, index=True)
    project_additional_data = Column(JSON)
    service_level = Column(String)
    welcome = Column(String)
