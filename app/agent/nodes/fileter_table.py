import yaml
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime
from app.agent.context import DataAgentContext
from app.agent.llm import llm
from app.agent.state import DataAgentState, TableInfoState
from app.prompt.prompt_loader import load_prompt
from app.core.log import logger


async def filter_table(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    step = "过滤表格"
    writer({"type": "progress", "step": step, "status": "running"})

    try:
        query = state["query"]
        table_infos = state["table_infos"]

        # 调用大模型筛选需要使用的表和字段
        prompt = PromptTemplate(template=load_prompt("filter_table_info"), input_variables=["query", "table_infos"])
        output_parser = JsonOutputParser()
        chain = prompt | llm | output_parser

        result = await chain.ainvoke(
            {"query": query, "table_infos": yaml.dump(table_infos, allow_unicode=True, sort_keys=False)})

        """
            返回的result格式为
            {
                "表名1":["字段1", "字段2", "..."],
                "表名2":["字段1", "字段2", "..."]
            }
            """
        # 过滤需要的表和字段
        filtered_table_infos: list[TableInfoState] = []

        for table_info in table_infos:
            if table_info["name"] in result:
                table_info["columns"] = [column_info for column_info in table_info["columns"] if column_info["name"] in result[table_info["name"]]]
                filtered_table_infos.append(table_info)

        table_info_names = [table_info["name"] for table_info in filtered_table_infos]
        logger.info(f"过滤之后的表信息: {table_info_names}")
        writer({"type": "progress", "step": step, "status": "success"})
        return {"table_infos": filtered_table_infos}
    except Exception as e:
        logger.info(f"过滤表信息异常：: {str(e)}")
        writer({"type": "progress", "step": step, "status": "error"})
        raise
