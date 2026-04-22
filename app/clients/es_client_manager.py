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
        await client.indices.create(
            index="books",
        )

        # add data to index
        await client.index(
            index="books",
            document={
                "name": "Snow Crash",
                "author": "Neal Stephenson",
                "release_date": "1992-06-01",
                "page_count": 470,
            },
        )

        # add multiple data to index
        await client.bulk(
            operations=[
                {
                    "index": {
                        "_index": "books"
                    }
                },
                {
                    "name": "Revelation Space",
                    "author": "Alastair Reynolds",
                    "release_date": "2000-03-15",
                    "page_count": 585
                },
                {
                    "index": {
                        "_index": "books"
                    }
                },
                {
                    "name": "1984",
                    "author": "George Orwell",
                    "release_date": "1985-06-01",
                    "page_count": 328
                },
                {
                    "index": {
                        "_index": "books"
                    }
                },
                {
                    "name": "Fahrenheit 451",
                    "author": "Ray Bradbury",
                    "release_date": "1953-10-15",
                    "page_count": 227
                },
                {
                    "index": {
                        "_index": "books"
                    }
                },
                {
                    "name": "Brave New World",
                    "author": "Aldous Huxley",
                    "release_date": "1932-06-01",
                    "page_count": 268
                },
                {
                    "index": {
                        "_index": "books"
                    }
                },
                {
                    "name": "The Handmaids Tale",
                    "author": "Margaret Atwood",
                    "release_date": "1985-06-01",
                    "page_count": 311
                }
            ],
        )


    asyncio.run(es_test())
