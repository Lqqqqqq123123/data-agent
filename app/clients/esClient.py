from elasticsearch import AsyncElasticsearch

from app.conf.app_config import ESConfig, app_config


class EsClientManager:
    def __init__(self):
        self.client: AsyncElasticsearch | None = None
        self.config: ESConfig | None = None

    def _get_url(self):
        return f'http://{self.config.host}:{self.config.port}'

    def init(self):
        self.config = app_config.es
        self.client = AsyncElasticsearch(
            hosts=[self._get_url()]
        )

    async def close(self):
        await self.client.close()


# 向外暴露
es_client_manager = EsClientManager()
es_client_manager.init()
if __name__ == '__main__':
    async def test():
        es_client_manager.init()
        client = es_client_manager.client

        if not await client.indices.exists(index='test_index'):
            resp = await client.indices.create(
                index='test_index',
            )
            print(resp)

        # resp = await client.index(
        #     index='test_index',
        #     document={
        #         "name": "test",
        #         "age": 18,
        #         "sex": "male"
        #     }
        # )
        resp = await client.delete_by_query(
            index='test_index',
            body={
                "query": {
                    "match": {
                        "name": "test"
                    }
                }
            }
        )
         
        print(resp)

        await es_client_manager.close()


    import asyncio

    asyncio.run(test())
