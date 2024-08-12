from fastapi import APIRouter, HTTPException
from models.api_models import Customer, CustomerClose
from typing import Optional, List
from utils.utils import get_authorization_header
from utils.logger import ModuleLogger
import httpx
import os

API_URL = os.getenv("API_URL")

router = APIRouter(
    prefix="/api/user",
    tags=["customers"],
)

logger = ModuleLogger("customers").get_logger()

@router.post("/add", response_model=dict)
async def add_customers(customers: List[Customer]):
    url = f"{API_URL}/api/user/add"
    headers = await get_authorization_header()
    data = {"users": [customer.dict() for customer in customers]}

    logger.info(f"Request to add customers: URL: {url}, Headers: {headers}, Data: {data}")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data, headers=headers)
    
    logger.info(f"Response from add customers: Status Code: {response.status_code}, Headers: {response.headers}, Body: {response.json()}")
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"Failed to add customers: {response.json()}")
    
    return response.json()

@router.post("/close", response_model=dict)
async def close_customers(customers: List[CustomerClose]):
    url = "https://api.infocus.company/api/user/close"
    headers = await get_authorization_header()
    data = {"users": [customer.dict() for customer in customers]}

    logger.info(f"Request to close customers: URL: {url}, Headers: {headers}, Data: {data}")

    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data, headers=headers)

    logger.info(f"Response from close customers: Status Code: {response.status_code}, Headers: {response.headers}, Body: {response.json()}")

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"Failed to close customers {response.json()}")
    
    return response.json()

@router.get("/list", response_model=List[dict])
async def get_customers(from_record: Optional[str] = None, id: Optional[str] = None, limit: Optional[str] = None, phone: Optional[str] = None):
    url = "https://api.infocus.company/api/user/list"
    headers = await get_authorization_header()

    params = {
        "from": from_record,
        "id": id,
        "limit": limit,
        "phone": phone
    }
    params = {k: v for k, v in params.items() if v is not None}

    logger.info(f"Request to get customers: URL: {url}, Headers: {headers}, Params: {params}")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers, params=params)

    logger.info(f"Response from get customers: Status Code: {response.status_code}, Headers: {response.headers}, Body: {response.json()}")

    if response.status_code != 200:
        logger.error(f"Error response from API: {response.text}")
        raise HTTPException(status_code=response.status_code, detail=f"Failed to get customer list: {response.text}")
    
    return response.json()


@router.post("/update", response_model=dict)
async def update_customers(customers: List[Customer]):
    url = "https://api.infocus.company/api/user/update"
    headers = await get_authorization_header()
    data = {"users": [customer.dict() for customer in customers]}

    logger.info(f"Request to update customers: URL: {url}, Headers: {headers}, Data: {data}")

    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data, headers=headers)

    logger.info(f"Response from update customers: Status Code: {response.status_code}, Headers: {response.headers}, Body: {response.json()}")

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"Failed to update customers {response.json()}")
    
    return response.json()
