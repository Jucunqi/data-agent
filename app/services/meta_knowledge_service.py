from pathlib import Path
from omegaconf import OmegaConf
from app.conf.meta_config import MetaConfig
from app.entities.column_info import ColumnInfo
from app.entities.table_info import TableInfo
from app.models.column_info_mysql import ColumnInfoMySQL
from app.models.table_info_mysql import TableInfoMySQL
from app.repositories.mysql.dw.dw_mysql_repository import DwMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository


class MetaKnowledgeService:
    def __init__(
            self,
            meta_mysql_repository: MetaMySQLRepository,
            dw_mysql_repository: DwMySQLRepository,
    ):
        self.meta_mysql_repository: MetaMySQLRepository = meta_mysql_repository
        self.dw_mysql_repository: DwMySQLRepository = dw_mysql_repository

    async def build(self, config_path: Path):

        # 读取配置文件
        context = OmegaConf.load(config_path)
        schema = OmegaConf.structured(MetaConfig)
        meta_config: MetaConfig = OmegaConf.to_object(OmegaConf.merge(schema, context))
        # 同步表信息和指标信息
        table_infos: list[TableInfo] = []
        # 获取表的字段信息
        column_infos: list[ColumnInfo] = []
        if meta_config.tables:
            # 定义一个数组，用于存储表信息
            # 配置信息中有表
            for table in meta_config.tables:
                table_info = TableInfo(
                    id=table.name, name=table.name, role=table.role,description=table.description
                )
                table_infos.append(table_info)

                # 查询字段类型
                column_types = await self.dw_mysql_repository.get_column_types(table.name)

                for column in table.columns:
                    # 查询字段取值示例
                    column_values = await self.dw_mysql_repository.get_column_values(table.name, column.name)

                    column_info = ColumnInfo(
                        id=f"{table.name}.{column.name}",
                        name=column.name,
                        type=column_types[column.name],
                        role=column.role,
                        examples=column_values,
                        description=column.description,
                        alias=column.alias,
                        table_id=table.name,
                    )
                    column_infos.append(column_info)

            # 插入表信息
            async with self.meta_mysql_repository.session.begin():
                self.meta_mysql_repository.save_table_infos(table_infos)
                # 插入字段信息
                self.meta_mysql_repository.save_column_infos(column_infos)

                # 对字段信息建立向量索引

                # 对指定字段添加全文索引
        if meta_config.metrics:
            # 配置信息中有指标信息

            # 将指标信息插入meta数据库中

            # 对指标建立向量索引
            pass
        pass
