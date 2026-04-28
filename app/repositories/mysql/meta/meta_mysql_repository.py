from sqlalchemy.ext.asyncio import AsyncSession

from app.entities.column_info import ColumnInfo
from app.entities.table_info import TableInfo
from app.repositories.mysql.meta.mappers.column_info_mapper import ColumnInfoMapper
from app.repositories.mysql.meta.mappers.table_info_mapper import TableInfoMapper


class MetaMySQLRepository:
    def __init__(self, session: AsyncSession):
        self.session: AsyncSession = session

    def save_table_infos(self, table_infos: list[TableInfo]):
        table_mysql_infos = [ TableInfoMapper.to_model(table_info) for table_info in table_infos]
        self.session.add_all(table_mysql_infos)

    def save_column_infos(self, column_infos: list[ColumnInfo]):
        column_mysql_infos = [ ColumnInfoMapper.to_model(column_info) for column_info in column_infos]
        self.session.add_all(column_mysql_infos)
