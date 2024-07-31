import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    API_URL = os.getenv("API_URL")
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    DB_URL = os.getenv("DB_URL")

settings = Settings()
