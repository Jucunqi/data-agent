from langgraph.runtime import Runtime
from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState, TableInfoState, MetricInfoState, ColumnInfoState
from app.entities.column_info import ColumnInfo
from app.entities.metric_info import MetricInfo
from app.entities.table_info import TableInfo
from app.entities.value_info import ValueInfo
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository


async def merge_retrieved_info(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("合并检索到的信息")

    retrieved_column_infos: list[ColumnInfo] = state["retrieved_column_infos"]
    retrieved_metric_infos: list[MetricInfo] = state["retrieved_metric_infos"]
    retrieved_value_infos: list[ValueInfo] = state["retrieved_value_infos"]
    meta_mysql_repository: MetaMySQLRepository = runtime.context["meta_mysql_repository"]

    # 处理表信息
    retrieved_column_infos_map: dict[str, ColumnInfo] = {retrieved_column_info.id: retrieved_column_info for
                                                         retrieved_column_info in retrieved_column_infos}
    for metric_info in retrieved_metric_infos:
        for relevant_column in metric_info.relevant_columns:
            if relevant_column not in retrieved_column_infos_map:
                column_info: ColumnInfo = await meta_mysql_repository.get_column_info_by_id(relevant_column)
                retrieved_column_infos_map[relevant_column] = column_info

    # 将字段取值加入column info的example中
    for retrieved_value_info in retrieved_value_infos:
        value = retrieved_value_info.value
        column_id = retrieved_value_info.column_id
        # 检查是否存在当前column
        if column_id not in retrieved_column_infos_map:
            column_info: ColumnInfo = await meta_mysql_repository.get_column_info_by_id(column_id)
            retrieved_column_infos_map[column_id] = column_info
        # 完善字段的example
        if value not in retrieved_column_infos_map[column_id].examples:
            retrieved_column_infos_map[column_id].examples.append(value)

    # 按照表对字段信息分组
    table_to_column_map: dict[str, list[ColumnInfo]] = {}
    for column_info in retrieved_column_infos_map.values():
        if column_info.table_id not in table_to_column_map:
            table_to_column_map[column_info.table_id] = []
        table_to_column_map[column_info.table_id].append(column_info)

    # 强制为表添加主外建字段
    for table_id in table_to_column_map.keys():
        key_columns:list[ColumnInfo] = await meta_mysql_repository.get_key_column_by_table_id(table_id)
        current_column_infos = table_to_column_map[table_id]
        column_ids = [current_column_info.id for current_column_info in current_column_infos]
        for key_column in key_columns:
            if key_column.id not in column_ids:
                current_column_infos.append(key_column)

    table_infos: list[TableInfoState] = []
    for table_id, column_infos in table_to_column_map.items():
        # 查询数据库获取table信息
        table_info: TableInfo = await meta_mysql_repository.get_table_info_by_id(table_id)
        columns = [ColumnInfoState(
            name=column_info.name,
            type=column_info.type,
            role=column_info.role,
            examples=column_info.examples,
            description=column_info.description,
            alias=column_info.alias
        ) for column_info in column_infos]

        table_info_state = TableInfoState(
            name=table_info.name,
            role=table_info.role,
            description=table_info.description,
            columns=columns
        )
        table_infos.append(table_info_state)

    # 处理指标信息
    metric_infos: list[MetricInfoState] = [MetricInfoState(
        name=retrieved_metric_info.name,
        description=retrieved_metric_info.description,
        relevant_columns=retrieved_metric_info.relevant_columns,
        alias=retrieved_metric_info.alias
    ) for retrieved_metric_info in retrieved_metric_infos]

    return {"table_infos": table_infos, "metric_infos": metric_infos}
