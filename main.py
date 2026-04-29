from fastapi import FastAPI

from app.api.lifespan import lifespan
from app.api.routers.query_router import query_router

app = FastAPI(lifespan=lifespan)

app.include_router(query_router)
