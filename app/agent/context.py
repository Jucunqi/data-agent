from typing import TypedDict

from app.clients.my_inference_client import MyInferenceClient
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw.dw_mysql_repository import DwMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository


class DataAgentContext(TypedDict):
    column_qdrant_repository: ColumnQdrantRepository
    embedding_client: MyInferenceClient
    metric_qdrant_repository: MetricQdrantRepository
    value_es_repository: ValueESRepository
    meta_mysql_repository: MetaMySQLRepository
    dw_mysql_repository: DwMySQLRepository
