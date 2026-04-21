"""
Elasticsearch client manager
"""

import asyncio
from app.conf.app_config import ESConfig, app_config
from elasticsearch import AsyncElasticsearch


class EsClientManager:
    def __init__(self, config: ESConfig):
        self.client: AsyncElasticsearch | None = None
        self.config: ESConfig = config

    def get_url(self) -> str:
        return f"http://{self.config.host}:{self.config.port}"

    def init(self):
        self.client = AsyncElasticsearch(hosts=[self.get_url()])

    async def close(self):
        if self.client:
            await self.client.close()


es_client_manager = EsClientManager(app_config.es)


if __name__ == "__main__":
    es_client_manager.init()

    async def es_test():

        client = es_client_manager.client
        if not client:
            raise ValueError("Elasticsearch client is not initialized")

        # 创建Index
        # await client.indices.create(
        #     index="books",
        # )

        # add data to index
        resp = await client.index(
            index="books",
            document={
                "name": "Snow Crash",
                "author": "Neal Stephenson",
                "release_date": "1992-06-01",
                "page_count": 470,
            },
        )

        print(resp)
        await es_client_manager.close()

    asyncio.run(es_test())

