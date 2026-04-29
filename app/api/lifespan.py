from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.clients.embedding_client_manager import embedding_client_manager
from app.clients.es_client_manager import es_client_manager
from app.clients.mysql_client_manager import meta_client_manager, dw_client_manager
from app.clients.qdrant_client_manager import qdrant_client_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    qdrant_client_manager.init()
    embedding_client_manager.init()
    es_client_manager.init()
    meta_client_manager.init()
    dw_client_manager.init()

    yield

    await qdrant_client_manager.close()
    await embedding_client_manager.close()
    await es_client_manager.close()
    await meta_client_manager.close()
    await dw_client_manager.close()
