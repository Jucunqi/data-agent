from app.agent.context import DataAgentContext
from app.agent.graph import graph
from app.agent.state import DataAgentState
from app.clients.my_inference_client import MyInferenceClient
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw.dw_mysql_repository import DwMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository


class QueryService:

    def __init__(self,
                 column_qdrant_repository: ColumnQdrantRepository,
                 embedding_client: MyInferenceClient,
                 metric_qdrant_repository: MetricQdrantRepository,
                 value_es_repository: ValueESRepository,
                 meta_mysql_repository: MetaMySQLRepository,
                 dw_mysql_repository: DwMySQLRepository):
        self.column_qdrant_repository = column_qdrant_repository
        self.embedding_client = embedding_client
        self.metric_qdrant_repository = metric_qdrant_repository
        self.value_es_repository = value_es_repository
        self.meta_mysql_repository = meta_mysql_repository
        self.dw_mysql_repository = dw_mysql_repository

    async def query(self, query: str):
        state = DataAgentState(**{"query": query})
        context = DataAgentContext(column_qdrant_repository=self.column_qdrant_repository,
                                   embedding_client=self.embedding_client,
                                   metric_qdrant_repository=self.metric_qdrant_repository,
                                   value_es_repository=self.value_es_repository,
                                   meta_mysql_repository=self.meta_mysql_repository,
                                   dw_mysql_repository=self.dw_mysql_repository)
        async for chunk in graph.astream(input=state, context=context, stream_mode="custom"):
            yield f"data: {chunk}\n\n"
