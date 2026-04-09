import asyncio

from app.clients.qdrantClient import qdrant_client_manager
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import VectorParams, Distance
from app.conf.app_config import app_config
from app.core.log import logger
from qdrant_client.models import PointStruct
class ColumnQdrantRepository:
    collection_name = 'column_info_collection'
    def __init__(self, client: AsyncQdrantClient):
        self.client = client

    async def create_collection(self):
        # 如果集合不存在，则创建集合
        if not await self.client.collection_exists(ColumnQdrantRepository.collection_name):
            await self.client.create_collection(
                collection_name=ColumnQdrantRepository.collection_name,
                vectors_config=VectorParams(size=app_config.qdrant.embedding_size, distance=Distance.COSINE)
            )
            logger.success(f'创建集合{ColumnQdrantRepository.collection_name}成功')
        else:
            logger.info(f'集合{ColumnQdrantRepository.collection_name}已存在')



    async def upsert(self, points:list[dict]):
        # 转换  dict --> PointStruct
        _points = [
            PointStruct(
                id=point['id'],
                vector=point['vector'],
                payload=point['payload']
            ) for point in points
        ]

        # 批量插入
        for i in range(0, len(_points), 20):
            operation_info = await self.client.upsert(
                collection_name=ColumnQdrantRepository.collection_name,
                points=_points[i:i+20]
            )
            logger.success(f'upsert成功，操作信息：{operation_info}')




if __name__ == '__main__':
    async def test():
        client = qdrant_client_manager.client
        await ColumnQdrantRepository(client).create_collection()
        await qdrant_client_manager.close()

    asyncio.run(test())