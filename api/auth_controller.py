from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from models.api_models import Token
from utils.config import settings
from utils.utils import get_token_from_db, save_token_to_db, update_token_in_db
from database.mysql import database
import requests

router = APIRouter(
    prefix="/api/oauth",
    tags=["auth"],
)

@router.post("/getToken", response_model=Token)
async def get_token():
    db = await database.get_pool()
    url = f"{settings.API_URL}/api/oauth/getToken"
    response = requests.post(url, json={"username": settings.LOGIN, "password": settings.PASSWORD})

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to get token")

    token_data = response.json()
    token = Token(
        login=settings.LOGIN,
        password=settings.PASSWORD,
        access_token=token_data['access_token'],
        refresh_token=token_data['refresh_token'],
        token_type=token_data['token_type'],
        expires_in=token_data['expires_in'],
        refresh_expires_in=token_data['refresh_expires_in'],
    )
    
    await save_token_to_db(db, token)
    return token

@router.post("/updateToken", response_model=Token)
async def update_token():
    db = await database.get_pool()
    token = await get_token_from_db(db)

    url = f"{settings.API_URL}/api/oauth/refreshToken"
    response = requests.post(url, json={"refresh_token": token.refresh_token})

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to update token")

    token_data = response.json()
    token.access_token = token_data['access_token']
    token.refresh_token = token_data['refresh_token']
    token.token_type = token_data['token_type']
    token.expires_in = token_data['expires_in']
    token.refresh_expires_in = token_data['refresh_expires_in']
    
    await update_token_in_db(db, token)
    return token
