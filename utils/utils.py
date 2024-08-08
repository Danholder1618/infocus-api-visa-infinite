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
    query = "SELECT * FROM tokens WHERE id = %s"
    result = await db.fetch_one(query, params=(1,), as_dict=True)
    return result

async def get_authorization_header(db):
    query = "SELECT access_token FROM tokens LIMIT 1"
    result = await db.fetch_one(query, as_dict=True)
    access_token = result['access_token'] if result else None
    return {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"} if access_token else {}

async def create_table_if_not_exists(pool):
    query = """
    CREATE TABLE IF NOT EXISTS tokens (
        id INT AUTO_INCREMENT PRIMARY KEY,
        login VARCHAR(255) NOT NULL,
        password VARCHAR(255) NOT NULL,
        access_token TEXT,
        refresh_token TEXT,
        token_type VARCHAR(50),
        expires_in INT,
        refresh_expires_in INT
    )
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query)
