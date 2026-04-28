import uuid
from dataclasses import asdict
from pathlib import Path

from langchain_huggingface import HuggingFaceEndpointEmbeddings
from omegaconf import OmegaConf
from app.conf.meta_config import MetaConfig
from app.entities.column_info import ColumnInfo
from app.entities.table_info import TableInfo
from app.entities.value_info import ValueInfo
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw.dw_mysql_repository import DwMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository


class MetaKnowledgeService:
    def __init__(
            self,
            meta_mysql_repository: MetaMySQLRepository,
            dw_mysql_repository: DwMySQLRepository,
            column_qdrant_repository: ColumnQdrantRepository,
            metric_qdrant_repository: MetricQdrantRepository,
            embedding_client: HuggingFaceEndpointEmbeddings,
            value_es_repository: ValueESRepository
    ):
        self.meta_mysql_repository: MetaMySQLRepository = meta_mysql_repository
        self.dw_mysql_repository: DwMySQLRepository = dw_mysql_repository
        self.column_qdrant_repository: ColumnQdrantRepository = column_qdrant_repository
        self.metric_qdrant_repository: MetricQdrantRepository = metric_qdrant_repository
        self.embedding_client: HuggingFaceEndpointEmbeddings = embedding_client
        self.value_es_repository: ValueESRepository = value_es_repository

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
                table_info = TableInfo(id=table.name, name=table.name, role=table.role, description=table.description)
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
            await self.column_qdrant_repository.ensure_collection()
            points: list[dict] = []
            for column_info in column_infos:
                points.append({
                    "id": uuid.uuid4(),
                    "embedding_text": column_info.name,
                    "payload": asdict(column_info)
                })

                points.append({
                    "id": uuid.uuid4(),
                    "embedding_text": column_info.description,
                    "payload": asdict(column_info)
                })

                for alia in column_info.alias:
                    points.append({
                        "id": uuid.uuid4(),
                        "embedding_text": alia,
                        "payload": asdict(column_info)
                    })
            # 向量化
            embeddings: list[list[float]] = []
            embedding_texts = [point["embedding_text"] for point in points]
            embedding_batch_size = 20
            for i in range(0, len(embedding_texts), embedding_batch_size):
                batch_embedding_texts = embedding_texts[i:i + embedding_batch_size]
                batch_embedding = await self.embedding_client.aembed_documents(batch_embedding_texts)
                embeddings.extend(batch_embedding)

            ids = [point["id"] for point in points]
            payloads = [point["payload"] for point in points]
            await self.column_qdrant_repository.upsert(ids, embeddings, payloads)

            # 对指定字段添加全文索引
            # 创建index
            await self.value_es_repository.ensure_index()

            # 构造ValueInfo列表
            value_infos: list[ValueInfo] = []
            for table in meta_config.tables:
                for column in table.columns:
                    if column.sync:
                        # 查询字段值
                        current_column_values = await self.dw_mysql_repository.get_column_values(table.name,
                                                                                                 column.name, 100000)
                        current_value_infos = [ValueInfo(id=f"{table.name}.{column.name}", value=column_value,
                                                         column_id=f"{table.name}.{column.name}") for column_value in
                                               current_column_values]
                        value_infos.extend(current_value_infos)

                        # 插入es
                        await self.value_es_repository.index(value_infos)




        if meta_config.metrics:
            # 配置信息中有指标信息

            # 将指标信息插入meta数据库中

            # 对指标建立向量索引
            pass
        pass
