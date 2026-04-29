from langgraph.runtime import Runtime
from app.core.log import logger
from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState


async def validate_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("验证SQL")

    sql = state["sql"]
    dw_mysql_repository = runtime.context["dw_mysql_repository"]

    try:
        await dw_mysql_repository.validate(sql)
        logger.info("验证SQL没有问题")
        return {"error": None}
    except Exception as e:
        logger.info(f"验证SQL存在问题：{str(e)}")
        return {"error": str(e)}
