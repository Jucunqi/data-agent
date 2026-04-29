import asyncio

from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.clients.my_inference_client import MyInferenceClient
from app.entities.column_info import ColumnInfo
from app.prompt.prompt_loader import load_prompt
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.core.log import logger


async def recall_column(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("召回列")

    keywords = state["keywords"]
    query = state["query"]
    column_qdrant_repository: ColumnQdrantRepository = runtime.context["column_qdrant_repository"]
    embedding_client: MyInferenceClient = runtime.context["embedding_client"]

    # 借助llm扩展关键词
    prompt_template = PromptTemplate(template=load_prompt(name="extend_keywords_for_column_recall"),
                                     input_variables=["query"])
    output_parser = JsonOutputParser()
    chain = prompt_template | llm | output_parser
    result = await chain.ainvoke({"query": query})
    keywords = set(keywords + result)

    # 在qdrant中检索关键词
    column_info_map: dict[str, ColumnInfo] = {}
    for keyword in keywords:
        # 获取向量
        embedding = await embedding_client.aembed_query(keyword)
        current_column_infos = await column_qdrant_repository.search(embedding, score_threshold=0.6, limit=5)
        for column_info in current_column_infos:
            if column_info.id not in column_info_map:
                column_info_map[column_info.id] = column_info

    retrieved_column_infos = list(column_info_map.values())
    logger.info(f"检索到的字段信息: {list(column_info_map.keys())}")
    return {"retrieved_column_infos": retrieved_column_infos}
