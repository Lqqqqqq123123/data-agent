from qdrant_client import AsyncQdrantClient
from qdrant_client.models import VectorParams, PointStruct, Distance

from app.conf.app_config import app_config
from app.core.log import logger

class MetricQdrantRepository:
    collection_name = 'metric_info_collection'
    def __init__(self, client: AsyncQdrantClient):
        self.client = client

    async def create_collection(self):
        # 如果集合不存在，则创建集合
        if not await self.client.collection_exists(collection_name=MetricQdrantRepository.collection_name):
            await self.client.create_collection(
                collection_name=MetricQdrantRepository.collection_name,
                vectors_config=VectorParams(
                    size=app_config.qdrant.embedding_size,
                    distance=Distance.COSINE
                )
            )
            logger.success(f'创建集合{MetricQdrantRepository.collection_name}成功')
        else:
            logger.info(f'集合{MetricQdrantRepository.collection_name}已存在')

    async def upsert(self, points:list[dict], batch_size=32):
        # 转换  dict --> PointStruct
        _points:list[PointStruct] = []

        for point in points:
            _points.append(
                PointStruct(
                    id=point['id'],
                    vector=point['vector'],
                    payload=point['payload']
                )
            )
        for i in range(0, len(_points), batch_size):
            resp = await self.client.upsert(
                collection_name=MetricQdrantRepository.collection_name,
                points=_points[i:i+batch_size]
            )
            logger.success(f'向集合{MetricQdrantRepository.collection_name}中插入数据成功，{resp}')






