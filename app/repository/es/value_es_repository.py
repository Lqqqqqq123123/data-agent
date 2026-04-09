import asyncio
from dataclasses import asdict

from elasticsearch import AsyncElasticsearch
from app.core.log import logger
from app.entities.value_info import ValueInfo


class ValueEsRepository:
    index_name = 'value_index'

    def __init__(self, client: AsyncElasticsearch):
        self.client = client

    async def create_index(self):
        # 判断是否存在
        if not await self.client.indices.exists(index=self.index_name):
            await self.client.indices.create(
                index=self.index_name,
                mappings={
                    "dynamic": False,
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "value": {
                            "type": "text",
                            'analyzer': "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "column_id": {
                            "type": "keyword"
                        }
                    }
                }
            )
            logger.info(f'创建索引 {self.index_name} 成功')
        else:
            logger.info(f'索引 {self.index_name} 已经存在')

    async def bulk(self, values: list, batch_size=64):
        '''
        批量保存值到 es 中
        :param batch_size:
        :param values:
        :return:
        '''

        for i in range(0, len(values), batch_size):
            operations = []
            for value in values[i:i + batch_size]:
                operations.append({"index": {"_index": self.index_name, "_id": value.id}})
                operations.append(asdict(value))

            await self.client.bulk(operations = operations)

            logger.info(f'批量保存 {len(values[i:i + batch_size])} 条数据成功')


if __name__ == '__main__':
    pass
