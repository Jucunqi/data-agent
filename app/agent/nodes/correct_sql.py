from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime
from app.core.log import logger
from app.agent.context import DataAgentContext
from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.prompt.prompt_loader import load_prompt


async def correct_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    step = "修正SQL"
    writer({"type": "progress", "step": step, "status": "running"})

    try:
        query = state["query"]
        table_infos = state["table_infos"]
        metric_infos = state["metric_infos"]
        date_info = state["date_info"]
        db_info = state["db_info"]
        sql = state["sql"]
        error = state["error"]

        # 借助llm生成sql
        prompt_template = PromptTemplate(template=load_prompt(name="correct_sql"),
                                         input_variables=["table_infos", "metric_infos", "date_info", "db_info",
                                                          "query", "sql", "error"])

        output_parser = StrOutputParser()
        chain = prompt_template | llm | output_parser
        result = await chain.ainvoke(
            {"table_infos": table_infos, "metric_infos": metric_infos, "date_info": date_info, "db_info": db_info,
             "query": query, "sql": sql, "error": error})
        logger.info(f"修改之后的sql：{result}")
        writer({"type": "progress", "step": step, "status": "success"})
        return {"sql": result}
    except Exception as e:
        logger.info(f"修正SQL异常: {str(e)}")
        writer({"type": "progress", "step": step, "status": "error"})
        raise
