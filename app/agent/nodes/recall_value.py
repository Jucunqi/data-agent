import asyncio

from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.entities.value_info import ValueInfo
from app.prompt.prompt_loader import load_prompt
from app.repositories.es.value_es_repository import ValueESRepository
from app.core.log import logger


async def recall_value(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("召回值")

    query = state["query"]
    keywords = state["keywords"]
    value_es_repository: ValueESRepository = runtime.context["value_es_repository"]

    # 借助llm扩展关键词
    prompt_template = PromptTemplate(template=load_prompt(name="extend_keywords_for_value_recall"),
                                     input_variables=["query"])
    output_parser = JsonOutputParser()
    chain = prompt_template | llm | output_parser
    result = await chain.ainvoke({"query": query})
    keywords = set(keywords + result)

    # 根据关键词召回字段取值
    current_value_map: dict[str, ValueInfo] = {}
    for keyword in keywords:
        current_value_infos: list[ValueInfo] = await value_es_repository.search(keyword=keyword, score_threshold=0.6,
                                                                                limit=5)
        for value_info in current_value_infos:
            if value_info.id not in current_value_map:
                current_value_map[value_info.id] = value_info

    retrieved_value_infos = list(current_value_map.values())
    logger.info(f"召回的值信息为: {list(current_value_map.keys())}")
    return {"retrieved_value_infos": retrieved_value_infos}
