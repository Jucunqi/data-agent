from qdrant_client import AsyncQdrantClient


class MetricQdrantRepository:
    def __init__(self, client: AsyncQdrantClient):
        self.client: AsyncQdrantClient = client
