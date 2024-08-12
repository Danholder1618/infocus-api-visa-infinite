from fastapi import APIRouter, HTTPException
from models.api_models import Token
from utils.utils import get_token_from_db, save_token_to_db, update_token_in_db
from database.mysql import database
from utils.logger import ModuleLogger
import httpx
import os

API_URL = os.getenv("API_URL")
LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")

router = APIRouter(
    prefix="/api/oauth",
    tags=["auth"],
)

logger = ModuleLogger("auth").get_logger()

@router.post("/getToken", response_model=Token)
async def get_token():
    url = f"{API_URL}/api/oauth/getToken"
    headers = {"Content-Type": "application/json"}
    data = {"username": LOGIN, "password": PASSWORD}

    logger.info(f"Request: URL: {url}, Headers: {headers},  Data: {data}")

    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data)
        
    logger.info(f"Response: Status Code: {response.status_code}, Headers: {response.headers}, Body: {response.json()}")

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"Failed to get token: {response.json()}")

    token_data = response.json()
    token = Token(
        login=LOGIN,
        password=PASSWORD,
        access_token=token_data['access_token'],
        refresh_token=token_data['refresh_token'],
        token_type=token_data['token_type'],
        expires_in=token_data['expires_in'],
        refresh_expires_in=token_data['refresh_expires_in'],
    )

    check = await get_token_from_db()
    if check:
        await update_token_in_db(token)
        return token

    await save_token_to_db(token)
    return token

@router.post("/updateToken", response_model=Token)
async def update_token():
    token = await get_token_from_db()

    url = f"{API_URL}/api/oauth/refreshToken"
    headers = {"Content-Type": "application/json"}
    data = {"refresh_token": token.refresh_token}
    
    logger.info(f"Request: URL: {url}, Headers: {headers},  Data: {data}")

    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data)

    logger.info(f"Response: Status Code: {response.status_code}, Headers: {response.headers}, Body: {response.json()}")

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"Failed to update token {response.json()}")
    
    token_data = response.json()
    token.access_token = token_data['access_token']
    token.refresh_token = token_data['refresh_token']
    token.token_type = token_data['token_type']
    token.expires_in = token_data['expires_in']
    token.refresh_expires_in = token_data['refresh_expires_in']
    
    await update_token_in_db(token)
    return token
