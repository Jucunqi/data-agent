from dataclasses import asdict

from elasticsearch import AsyncElasticsearch

from app.entities.value_info import ValueInfo


class ValueESRepository:
    index_name = "value_index"
    index_mappings = {
        "dynamic": False,
        "properties": {
            "id": {"type": "keyword"},
            "value": {"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_max_word"},
            "column_id": {"type": "keyword"}
        }
    }

    def __init__(self, client: AsyncElasticsearch):
        self.client: AsyncElasticsearch = client

    async def ensure_index(self):
        """
        如果不存在则创建
        """
        if not await self.client.indices.exists(index=self.index_name):
            await self.client.indices.create(
                index=self.index_name,
                mappings=self.index_mappings
            )

    async def index(self, value_infos: list[ValueInfo], batch_size=20):
        for i in range(0, len(value_infos), batch_size):
            current_value_infos = value_infos[i: i + batch_size]

            operations: list[dict] = []
            for value_info in current_value_infos:
                operations.append(
                    {
                        "index": {
                            "_index": self.index_name
                        }
                    }
                )
                operations.append(
                    asdict(value_info)
                )

            await self.client.bulk(
                index=self.index_name,
                operations=operations
            )

    async def search(self, keyword, score_threshold, limit) -> list[ValueInfo]:
        resp = await self.client.search(
            index=self.index_name,
            query={
                "match": {
                    "value": keyword
                }
            },
            size=limit,
            min_score=score_threshold
        )

        return [ValueInfo(**hit["_source"]) for hit in resp["hits"]["hits"]]
