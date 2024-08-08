from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import auth_controller, customers_controller
from database.mysql import database
from utils.utils import create_table_if_not_exists
import logging

app = FastAPI()

app.include_router(auth_controller.router)
app.include_router(customers_controller.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    try:
        await database.connect()
        await create_table_if_not_exists(database.pool)
    except Exception as e:
        logging.error(f"Error during startup: {e}")

@app.on_event("shutdown")
async def shutdown():
    try:
        await database.close()
    except Exception as e:
        logging.error(f"Error during shutdown: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=6969)
