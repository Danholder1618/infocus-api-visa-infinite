from sqlalchemy.orm import Session
from models.api_models import Customer
from models.db_models import User
from models.db_models import Token

def save_token_to_db(db: Session, token: Token):
    db.add(token)
    db.commit()
    db.refresh(token)

def update_token_in_db(db: Session, token: Token):
    db.commit()
    db.refresh(token)

def get_token_from_db(db: Session):
    return db.query(Token).first()

def get_authorization_header(db: Session):
    token = get_token_from_db(db)
    return {"Authorization": f"Bearer {token.access_token}", "Content-Type": "application/json"}
