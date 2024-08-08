from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from api import auth_controller, customers_controller
from database.mysql import database

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
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=6969)
