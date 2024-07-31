from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from api import vsm
from database.mysql import get_mysql_client

app = FastAPI()

app.include_router(vsm.router, prefix="/infocus")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    mysql_client = get_mysql_client()
    await mysql_client.create_pool()

@app.on_event("shutdown")
async def shutdown():
    mysql_client = get_mysql_client()
    await mysql_client.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=6969)
