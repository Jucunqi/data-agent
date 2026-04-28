from qdrant_client import AsyncQdrantClient
from qdrant_client.http.models import PointStruct
from qdrant_client.models import VectorParams, Distance

from app.conf.app_config import app_config


class ColumnQdrantRepository:
    collection_name = "column_info_collection"

    def __init__(self, client: AsyncQdrantClient):
        self.client: AsyncQdrantClient = client

    async def ensure_collection(self):
        if not await self.client.collection_exists(self.collection_name):
            await self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(size=app_config.qdrant.embedding_size, distance=Distance.COSINE),
            )

    async def upsert(self, ids: list[str], embeddings: list[list[float]], payloads: list[dict], batch_size: int = 10):
        points: list[PointStruct] = [PointStruct(id=id, vector=embedding, payload=payload) for id, embedding, payload in
                                     zip(ids, embeddings, payloads)]

        for i in range(0, len(points), batch_size):
            batch_points = points[i: i + batch_size]
            await self.client.upsert(
                collection_name=self.collection_name,
                points=batch_points
            )
