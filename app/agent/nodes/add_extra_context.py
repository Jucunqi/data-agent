from datetime import date
from app.core.log import logger
from langgraph.runtime import Runtime
from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState, DateInfoState, DBInfoState


async def add_extra_context(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("添加额外上下文")

    today = date.today()
    date_str = today.strftime("%Y-%m-%d")
    weekday = today.strftime("%A")
    quarter = f"Q{(today.month - 1) // 3 + 1}"

    date_info = DateInfoState(date=date_str, weekday=weekday, quarter=quarter)

    # 获取数据库信息
    dw_mysql_repository = runtime.context["dw_mysql_repository"]
    db = await dw_mysql_repository.get_db_info()
    db_info = DBInfoState(**db)

    logger.info(f"日期信息：{date_info}")
    logger.info(f"数据库信息：{db_info}")
    return {"date_info": date_info, "db_info": db_info}
