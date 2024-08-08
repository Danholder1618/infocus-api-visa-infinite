from sqlalchemy.orm import Session
from models.db_models import Token

async def save_token_to_db(db, token: Token):
    query = """
    INSERT INTO tokens (login, password, access_token, refresh_token, token_type, expires_in, refresh_expires_in)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    params = (token.login, token.password, token.access_token, token.refresh_token, token.token_type, token.expires_in, token.refresh_expires_in)
    await db.execute_query(query, params)

async def update_token_in_db(db, token: Token):
    query = """
    UPDATE tokens SET access_token = %s, refresh_token = %s, token_type = %s, expires_in = %s, refresh_expires_in = %s
    WHERE login = %s
    """
    params = (token.access_token, token.refresh_token, token.token_type, token.expires_in, token.refresh_expires_in, token.login)
    await db.execute_query(query, params)

async def get_token_from_db(db):
    query = "SELECT * FROM tokens LIMIT 1"
    result = await db.fetch_one(query, as_dict=True)
    return Token(**result) if result else None

def get_authorization_header(db: Session):
    token = get_token_from_db(db)
    return {"Authorization": f"Bearer {token.access_token}", "Content-Type": "application/json"}
