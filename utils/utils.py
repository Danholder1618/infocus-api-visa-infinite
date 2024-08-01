import requests
from sqlalchemy.orm import Session
from models.api_models import Customer
from models.db_models import User
from .config import settings

def get_token(username: str, password: str):
    url = f"{settings.API_URL}/api/oauth/getToken"
    response = requests.post(url, json={"username": username, "password": password})
    if response.status_code == 200:
        return response.json()
    return None

def create_customer(db: Session, customer: Customer):
    db_customer = Customer(
        additional_phone=customer.additional_phone,
        bank_manager_fio=customer.bank_manager_fio,
        bank_manager_phone=customer.bank_manager_phone,
        bank_product=customer.bank_product,
        bin=customer.bin,
        card_type_id=customer.card_type_id,
        clid=customer.clid,
        date_birth=customer.date_birth,
        date_expiry=customer.date_expiry,
        email=customer.email,
        firstname=customer.firstname,
        inn=customer.inn,
        language=customer.language,
        lastname=customer.lastname,
        manager=customer.manager,
        manualSubscribe=customer.manualSubscribe,
        messageId=customer.messageId,
        middlename=customer.middlename,
        pan=customer.pan,
        phone=customer.phone,
        project_additional_data=customer.project_additional_data,
        service_level=customer.service_level,
        welcome=customer.welcome,
    )
    db.add(db_customer)
    db.commit()
    db.refresh(db_customer)
    return db_customer

def create_user(db: Session, user: User):
    db_user = User(
        username=user.username,
        password=user.password,
        access_token=user.access_token,
        expires_in=user.expires_in,
        refresh_expires_in=user.refresh_expires_in,
        refresh_token=user.refresh_token,
        token_type=user.token_type,
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user
