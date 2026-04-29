from typing import Annotated

from elasticsearch import AsyncElasticsearch
from fastapi.params import Depends
from qdrant_client import AsyncQdrantClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.embedding_client_manager import embedding_client_manager
from app.clients.es_client_manager import es_client_manager
from app.clients.my_inference_client import MyInferenceClient
from app.clients.mysql_client_manager import meta_client_manager, dw_client_manager
from app.clients.qdrant_client_manager import qdrant_client_manager
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw.dw_mysql_repository import DwMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository
from app.services.query_service import QueryService


async def get_qdrant_client() -> AsyncQdrantClient:
    return qdrant_client_manager.client


async def get_column_qdrant_repository(
        client: Annotated[AsyncQdrantClient, Depends(get_qdrant_client)]) -> ColumnQdrantRepository:
    return ColumnQdrantRepository(client)


async def get_embedding_client() -> MyInferenceClient:
    return embedding_client_manager.client


async def get_metric_qdrant_repository(
        client: Annotated[AsyncQdrantClient, Depends(get_qdrant_client)]) -> MetricQdrantRepository:
    return MetricQdrantRepository(client)


async def get_async_es() -> AsyncElasticsearch:
    return es_client_manager.client


async def get_value_es_repository(client: Annotated[AsyncElasticsearch, Depends(get_async_es)]) -> ValueESRepository:
    return ValueESRepository(client)


async def get_meta_session():
    async with meta_client_manager.session_factory() as session:
        yield session


async def get_meta_mysql_repository(session: Annotated[AsyncSession, Depends(get_meta_session)]) -> MetaMySQLRepository:
    return MetaMySQLRepository(session=session)


async def get_dw_session():
    async with dw_client_manager.session_factory() as session:
        yield session


async def get_dw_mysql_repository(session: Annotated[AsyncSession, Depends(get_dw_session)]) -> DwMySQLRepository:
    return DwMySQLRepository(session)


async def get_query_service(
        column_qdrant_repository: Annotated[ColumnQdrantRepository, Depends(get_column_qdrant_repository)],
        embedding_client: Annotated[MyInferenceClient, Depends(get_embedding_client)],
        metric_qdrant_repository: Annotated[MetricQdrantRepository, Depends(get_metric_qdrant_repository)],
        value_es_repository: Annotated[ValueESRepository, Depends(get_value_es_repository)],
        meta_mysql_repository: Annotated[MetaMySQLRepository, Depends(get_meta_mysql_repository)],
        dw_mysql_repository: Annotated[DwMySQLRepository, Depends(get_dw_mysql_repository)]
) -> QueryService:
    return QueryService(
        column_qdrant_repository=column_qdrant_repository,
        embedding_client=embedding_client,
        metric_qdrant_repository=metric_qdrant_repository,
        value_es_repository=value_es_repository,
        meta_mysql_repository=meta_mysql_repository,
        dw_mysql_repository=dw_mysql_repository
    )
