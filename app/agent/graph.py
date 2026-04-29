import asyncio

from langgraph.constants import END
from langgraph.graph import START, StateGraph

from app.agent.context import DataAgentContext
from app.agent.nodes.add_extra_context import add_extra_context
from app.agent.nodes.correct_sql import correct_sql
from app.agent.nodes.extract_keywords import extract_keywords
from app.agent.nodes.fileter_table import filter_table
from app.agent.nodes.filter_metric import filter_metric
from app.agent.nodes.generate_sql import generate_sql
from app.agent.nodes.merge_retrieved_info import merge_retrieved_info
from app.agent.nodes.recall_column import recall_column
from app.agent.nodes.recall_metric import recall_metric
from app.agent.nodes.recall_value import recall_value
from app.agent.nodes.run_sql import run_sql
from app.agent.nodes.validate_sql import validate_sql
from app.agent.state import DataAgentState
from app.clients.embedding_client_manager import embedding_client_manager
from app.clients.es_client_manager import es_client_manager
from app.clients.mysql_client_manager import meta_client_manager, dw_client_manager
from app.clients.qdrant_client_manager import qdrant_client_manager
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw import dw_mysql_repository
from app.repositories.mysql.dw.dw_mysql_repository import DwMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository

# 构建状态图
graph_builder = StateGraph(DataAgentState)

# 添加节点
graph_builder.add_node("extract_keywords", extract_keywords)
graph_builder.add_node("recall_column", recall_column)
graph_builder.add_node("recall_metric", recall_metric)
graph_builder.add_node("recall_value", recall_value)
graph_builder.add_node("merge_retrieved_info", merge_retrieved_info)
graph_builder.add_node("filter_metric", filter_metric)
graph_builder.add_node("filter_table", filter_table)
graph_builder.add_node("add_extracted_keywords", add_extra_context)
graph_builder.add_node("generate_sql", generate_sql)
graph_builder.add_node("validate_sql", validate_sql)
graph_builder.add_node("correct_sql", correct_sql)
graph_builder.add_node("run_sql", run_sql)

# 添加边
graph_builder.add_edge(START, "extract_keywords")
graph_builder.add_edge("extract_keywords", "recall_column")
graph_builder.add_edge("extract_keywords", "recall_metric")
graph_builder.add_edge("extract_keywords", "recall_value")
graph_builder.add_edge("recall_column", "merge_retrieved_info")
graph_builder.add_edge("recall_metric", "merge_retrieved_info")
graph_builder.add_edge("recall_value", "merge_retrieved_info")
graph_builder.add_edge("merge_retrieved_info", "filter_metric")
graph_builder.add_edge("merge_retrieved_info", "filter_table")
graph_builder.add_edge("filter_metric", "add_extracted_keywords")
graph_builder.add_edge("filter_table", "add_extracted_keywords")
graph_builder.add_edge("add_extracted_keywords", "generate_sql")
graph_builder.add_edge("generate_sql", "validate_sql")
graph_builder.add_conditional_edges(
    "validate_sql",
    lambda state: "run_sql" if not state["error"] else "correct_sql",
    {"run_sql": "run_sql", "correct_sql": "correct_sql"},
)
graph_builder.add_edge("correct_sql", "run_sql")
graph_builder.add_edge("run_sql", END)

graph = graph_builder.compile()

if __name__ == '__main__':
    async def test():

        qdrant_client_manager.init()
        embedding_client_manager.init()
        es_client_manager.init()
        meta_client_manager.init()
        dw_client_manager.init()

        async with meta_client_manager.session_factory() as meta_session:
            column_qdrant_repository = ColumnQdrantRepository(qdrant_client_manager.client)
            metric_qdrant_repository = MetricQdrantRepository(qdrant_client_manager.client)
            value_es_repository = ValueESRepository(es_client_manager.client)
            meta_mysql_repository = MetaMySQLRepository(meta_session)
            dw_mysql_repository = DwMySQLRepository(meta_session)

            state = DataAgentState(**{"query": "查询华北地区的销售总额"})
            context = DataAgentContext(column_qdrant_repository=column_qdrant_repository,
                                       embedding_client=embedding_client_manager.client,
                                       metric_qdrant_repository=metric_qdrant_repository,
                                       value_es_repository=value_es_repository,
                                       meta_mysql_repository=meta_mysql_repository,
                                       dw_mysql_repository=dw_mysql_repository)
            async for chunk in graph.astream(input=state, context=context, stream_mode="custom"):
                print(chunk)

        await qdrant_client_manager.close()


    asyncio.run(test())
