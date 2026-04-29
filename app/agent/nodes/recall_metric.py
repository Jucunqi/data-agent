import asyncio

from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.entities.metric_info import MetricInfo
from app.prompt.prompt_loader import load_prompt
from app.core.log import logger
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository


async def recall_metric(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    step = "召回指标"
    writer({"type": "progress", "step": step, "status": "running"})

    try:
        query = state["query"]
        keywords = state["keywords"]
        embedding_client = runtime.context["embedding_client"]
        metric_qdrant_repository: MetricQdrantRepository = runtime.context["metric_qdrant_repository"]

        # 借助llm扩展关键词
        prompt_template = PromptTemplate(template=load_prompt(name="extend_keywords_for_metric_recall"),
                                        input_variables=["query"])
        output_parser = JsonOutputParser()
        chain = prompt_template | llm | output_parser
        result = await chain.ainvoke({"query": query})
        keywords = set(keywords + result)

        # 根据关键词检索指标信息
        metric_info_map: dict[str, MetricInfo] = {}
        for keyword in keywords:
            # 获取向量
            embedding = await embedding_client.aembed_query(keyword)
            metric_column_infos = await metric_qdrant_repository.search(embedding, score_threshold=0.6, limit=5)
            for metric_info in metric_column_infos:
                if metric_info.id not in metric_info_map:
                    metric_info_map[metric_info.id] = metric_info

        retrieved_metric_infos = list(metric_info_map.values())
        logger.info(f"检索到的指标信息: {list(metric_info_map.keys())}")
        writer({"type": "progress", "step": step, "status": "success"})
        return {"retrieved_metric_infos": retrieved_metric_infos}
    except Exception as e:
        logger.info(f"召回指标存在问题：{str(e)}")
        writer({"type": "progress", "step": step, "status": "error"})
        return {"error": str(e)}
