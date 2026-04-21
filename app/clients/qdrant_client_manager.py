"""
Qdrant 客户端管理类
"""
import asyncio
from qdrant_client import AsyncQdrantClient
from app.conf.app_config import QdrantConfig, app_config    


class QdrantClientManager:
    def __init__(self, config: QdrantConfig):
        self.client: AsyncQdrantClient | None = None
        self.config: QdrantConfig = config

    def get_url(self) -> str:
        return f"http://{self.config.host}:{self.config.port}"

    def init(self):
        self.client = AsyncQdrantClient(url=self.get_url())

    async def close(self):
        if self.client:
            await self.client.close()


qdrant_client_manager = QdrantClientManager(app_config.qdrant)

if __name__ == "__main__":
    qdrant_client_manager.init()

    client = qdrant_client_manager.client

    async def qdrant_test():
        # 创建一个集合
        from qdrant_client.models import Distance, VectorParams

        if not client:
            raise ValueError("Qdrant client is not initialized")

        await client.create_collection(
            collection_name="test_collection_async",
            vectors_config=VectorParams(size=4, distance=Distance.COSINE),
        )

        # 添加向量
        from qdrant_client.models import PointStruct

        await client.upsert(
            collection_name="test_collection_async",
            wait=True,
            points=[
                PointStruct(id=1, vector=[0.05, 0.61, 0.76, 0.74], payload={"city": "Berlin"}),
                PointStruct(id=2, vector=[0.19, 0.81, 0.75, 0.11], payload={"city": "London"}),
                PointStruct(id=3, vector=[0.36, 0.55, 0.47, 0.94], payload={"city": "Moscow"}),
                PointStruct(id=4, vector=[0.18, 0.01, 0.85, 0.80], payload={"city": "New York"}),
                PointStruct(id=5, vector=[0.24, 0.18, 0.22, 0.44], payload={"city": "Beijing"}),
                PointStruct(id=6, vector=[0.35, 0.08, 0.11, 0.44], payload={"city": "Mumbai"}),
            ],
        )

        # 查询结果
        search_result = await client.query_points(
            collection_name="test_collection_async",
            query=[0.2, 0.1, 0.9, 0.7],
            with_payload=False,
            limit=3
        )
        return search_result

    print(asyncio.run(qdrant_test()))
