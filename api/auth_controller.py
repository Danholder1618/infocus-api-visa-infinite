from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import OAuth2PasswordRequestForm
from models.db_models import Token
from utils.utils import get_token

router = APIRouter(
    prefix="/api/oauth",
    tags=["auth"],
)

@router.post("/getToken", response_model=Token)
async def get_token(form_data: OAuth2PasswordRequestForm = Depends()):
    token = get_token(form_data.username, form_data.password)
    if not token:
        raise HTTPException(status_code=400, detail="Invalid credentials")
    return token
