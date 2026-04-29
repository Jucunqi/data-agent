from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.entities.column_info import ColumnInfo
from app.entities.table_info import TableInfo
from app.models.column_info_mysql import ColumnInfoMySQL
from app.models.metric_info_mysql import MetricInfoMySQL
from app.models.table_info_mysql import TableInfoMySQL
from app.repositories.mysql.meta.mappers.column_info_mapper import ColumnInfoMapper
from app.repositories.mysql.meta.mappers.column_metric_mapper import ColumnMetricMapper
from app.repositories.mysql.meta.mappers.metric_info_mapper import MetricInfoMapper
from app.repositories.mysql.meta.mappers.table_info_mapper import TableInfoMapper


class MetaMySQLRepository:
    def __init__(self, session: AsyncSession):
        self.session: AsyncSession = session

    def save_table_infos(self, table_infos: list[TableInfo]):
        table_mysql_infos = [TableInfoMapper.to_model(table_info) for table_info in table_infos]
        self.session.add_all(table_mysql_infos)

    def save_column_infos(self, column_infos: list[ColumnInfo]):
        column_mysql_infos = [ColumnInfoMapper.to_model(column_info) for column_info in column_infos]
        self.session.add_all(column_mysql_infos)

    def save_metric_infos(self, metric_infos):
        metric_mysql_infos = [MetricInfoMapper.to_model(metric_info) for metric_info in metric_infos]
        self.session.add_all(metric_mysql_infos)

    def save_column_metrics(self, column_metrics):
        column_metrics_mysql = [ColumnMetricMapper.to_model(column_metric) for column_metric in column_metrics]
        self.session.add_all(column_metrics_mysql)

    async def get_column_info_by_id(self, relevant_column) -> ColumnInfo | None:
        column_info_mysql: ColumnInfoMySQL | None = await self.session.get(ColumnInfoMySQL, relevant_column)
        if column_info_mysql:
            return ColumnInfoMapper.to_entity(column_info_mysql)
        else:
            return None

    async def get_table_info_by_id(self, table_id) -> TableInfo | None:
        table_info_mysql: TableInfoMySQL | None = await self.session.get(TableInfoMySQL, table_id)
        if table_info_mysql:
            return TableInfoMapper.to_entity(table_info_mysql)
        else:
            return None

    async def get_key_column_by_table_id(self, table_id) -> list[ColumnInfo]:
        """
        查询表的主外建信息
        :param table_id: 表
        :return: 主外建字段
        """
        sql = "select * from column_info where table_id = :table_id and role in ('primary_key','foreign_key')"
        result = await self.session.execute(text(sql), {"table_id": table_id})
        return [ColumnInfo(**dict(row)) for row in result.mappings().fetchall()]
