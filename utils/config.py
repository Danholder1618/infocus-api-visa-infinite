import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    API_URL = os.getenv("API_URL")
    LOGIN = os.getenv("LOGIN")
    PASSWORD = os.getenv("PASSWORD")
    DB_URL = os.getenv("DB_URL")

settings = Settings()
