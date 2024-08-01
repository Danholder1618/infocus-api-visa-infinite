from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from database.mysql import get_mysql_client
from models.db_models import CustomerCreate, Customer
from models.db_models import Customer as CustomerModel
from utils import create_customer

router = APIRouter(
    prefix="/api/customers",
    tags=["customers"],
)

@router.post("/", response_model=Customer)
async def add_customer(customer: CustomerCreate, db: Session = Depends(get_mysql_client)):
    db_customer = create_customer(db, customer)
    if not db_customer:
        raise HTTPException(status_code=400, detail="Customer creation failed")
    return db_customer
