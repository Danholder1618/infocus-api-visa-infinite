from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from models.api_models import Customer, CustomerClose
from typing import Optional, List
from utils.config import settings
from database.mysql import get_mysql_client
from utils.utils import get_authorization_header
import requests

router = APIRouter(
    prefix="/api/customers",
    tags=["customers"],
)

@router.post("/add", response_model=dict)
async def add_customers(customers: List[Customer], db: Session = Depends(get_mysql_client().Session)):
    url = f"{settings.API_URL}/api/user/add"
    headers = get_authorization_header(db)
    
    response = requests.post(url, json={"users": [customer.dict() for customer in customers]}, headers=headers)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to add customers")
    
    return response.json()

@router.post("/close", response_model=dict)
async def close_customers(customers: List[CustomerClose], db: Session = Depends(get_mysql_client().Session)):
    url = "https://api.infocus.company/api/user/close"
    headers = get_authorization_header(db)
    
    response = requests.post(url, json={"users": [customer.dict() for customer in customers]}, headers=headers)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to close customers")
    
    return response.json()

@router.get("/list", response_model=dict)
async def get_customers(from_record: Optional[str] = None, id: Optional[str] = None, limit: Optional[str] = None, phone: Optional[str] = None, db: Session = Depends(get_mysql_client().Session)):
    url = "https://api.infocus.company/api/user/list"
    headers = get_authorization_header(db)
    params = {"from": from_record, "id": id, "limit": limit, "phone": phone}
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to get customer list")
    
    return response.json()

@router.post("/update", response_model=dict)
async def update_customers(customers: List[Customer], db: Session = Depends(get_mysql_client().Session)):
    url = "https://api.infocus.company/api/user/update"
    headers = get_authorization_header(db)
    
    response = requests.post(url, json={"users": [customer.dict() for customer in customers]}, headers=headers)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to update customers")
    
    return response.json()
