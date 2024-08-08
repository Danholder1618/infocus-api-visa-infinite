from fastapi import APIRouter, HTTPException
from models.api_models import Customer, CustomerClose
from typing import Optional, List
from database.mysql import database
from utils.utils import get_authorization_header
import requests
import os

API_URL = os.getenv("API_URL")

router = APIRouter(
    prefix="/api/customers",
    tags=["customers"],
)

@router.post("/add", response_model=dict)
async def add_customers(customers: List[Customer]):
    url = f"{API_URL}/api/user/add"
    headers = get_authorization_header(database)
    
    response = requests.post(url, json={"users": [customer.dict() for customer in customers]}, headers=headers)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to add customers")
    
    return response.json()

@router.post("/close", response_model=dict)
async def close_customers(customers: List[CustomerClose]):
    url = "https://api.infocus.company/api/user/close"
    headers = get_authorization_header(database)
    
    response = requests.post(url, json={"users": [customer.dict() for customer in customers]}, headers=headers)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to close customers")
    
    return response.json()

@router.get("/list", response_model=dict)
async def get_customers(from_record: Optional[str] = None, id: Optional[str] = None, limit: Optional[str] = None, phone: Optional[str] = None):
    url = "https://api.infocus.company/api/user/list"
    headers = get_authorization_header(database)
    params = {"from": from_record, "id": id, "limit": limit, "phone": phone}
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to get customer list")
    
    return response.json()

@router.post("/update", response_model=dict)
async def update_customers(customers: List[Customer]):
    url = "https://api.infocus.company/api/user/update"
    headers = get_authorization_header(database)
    
    response = requests.post(url, json={"users": [customer.dict() for customer in customers]}, headers=headers)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to update customers")
    
    return response.json()
