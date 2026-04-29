from langgraph.runtime import Runtime
from app.core.log import logger
from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState


async def run_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    step = "运行SQL"
    writer({"type": "progress", "step": step, "status": "running"})

    try:

        sql = state["sql"]
        dw_mysql_repository = runtime.context["dw_mysql_repository"]

        result = await dw_mysql_repository.run_sql(sql)
        logger.info(f"执行SQL结果: {result}")
        writer({"type": "progress", "step": step, "status": "success"})
        writer({"type": "result", "data": result})
    except Exception as e:
        logger.info(f"运行SQL存在问题：{str(e)}")
        writer({"type": "progress", "step": step, "status": "error"})
        raise