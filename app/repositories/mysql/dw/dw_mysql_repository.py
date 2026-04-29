from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


class DwMySQLRepository:
    def __init__(self, session: AsyncSession):
        self.session: AsyncSession = session

    async def get_column_types(self, table_name: str):
        """
        查询指定表的所有字段类型
        """
        sql = f"show columns from {table_name}"
        result = await self.session.execute(text(sql))
        result_dict = result.mappings().fetchall()

        # 最终的结构是一个字典，格式为 {"字段名": "字段类型"}
        return {item["Field"]: item["Type"] for item in result_dict}

    async def get_column_values(self, table_name: str, column_name: str, limit: int = 10):
        """
        查询指定表指定字段的所有取值示例
        """
        sql = f"select distinct {column_name} from {table_name} limit {limit}"
        result = await self.session.execute(text(sql))
        result_dict = result.fetchall()
        # 最终的结构是一个列表，每个元素是一个字典，包含字段值
        return [row[0] for row in result_dict]

    async def get_db_info(self):
        """
        获取数据库的版本和方言信息
        :return: 字典
        """
        sql = "select version()"
        resp = await self.session.execute(text(sql))
        version = resp.scalar()
        dialect = self.session.bind.dialect.name
        return {"dialect": dialect, "version": version}
