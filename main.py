from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import auth_controller, customers_controller
from database.mysql import database
from utils.utils import create_table_if_not_exists, load_customers_from_file, process_customers
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
import asyncio

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

scheduler = AsyncIOScheduler()

async def update_tokens():
    await auth_controller.update_token()

async def load_data():
    customers = await load_customers_from_file('./data/new_data.json')
    await process_customers(customers)

async def check_and_update_customers():
    await load_data()

@app.on_event("startup")
async def startup():
    try:
        await database.connect()
        await create_table_if_not_exists()
        await auth_controller.get_token()
        scheduler.add_job(update_tokens, CronTrigger(day="*/25"))
        await check_and_update_customers()
        scheduler.add_job(check_and_update_customers, CronTrigger(day="*/1"))
        
        scheduler.start()
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
