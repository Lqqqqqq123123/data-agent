import asyncio

from qdrant_client import AsyncQdrantClient
from app.conf.app_config import QdrantConfig, app_config
class QdrantClientManager:
    def __init__(self):
        self.client: AsyncQdrantClient | None = None
        self.config: QdrantConfig | None = None

    def _get_url(self):
        return f'http://{self.config.host}:{self.config.port}'

    def init(self):
        self.config = app_config.qdrant
        self.client = AsyncQdrantClient(url=self._get_url())

    async def close(self):
        await self.client.close()

qdrant_client_manager = QdrantClientManager()

if __name__ == '__main__':
    async def test():
        qdrant_client_manager.init()
        client = qdrant_client_manager.client

        from qdrant_client.models import VectorParams, Distance
        resp = await client.create_collection(
            collection_name="test_collection1",
            vectors_config=VectorParams(size=4, distance=Distance.DOT)
        )

        print(resp)

    asyncio.run(test())