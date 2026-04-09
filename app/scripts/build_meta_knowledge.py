import argparse
import asyncio
import sys
from pathlib import Path

from app.clients.embedClient import embedding_client_manager
from app.clients.mysqlClient import meta_mysql_client_manager, dw_mysql_client_manager
from app.clients.qdrantClient import qdrant_client_manager
from app.core.log import logger
from app.repository.es.value_es_repository import ValueEsRepository
from app.repository.mysql.dw.dw_mysql_repository import DwMysqlRepository
from app.repository.mysql.meta.meta_mysql_repository import MetaMysqlRepository
from app.repository.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.clients.esClient import es_client_manager
from app.repository.qdrant.metric_qdrant_repository import MetricQdrantRepository


async def build(conf_path: Path | None):
    logger.success(f'读取到的配置文件路径：{conf_path}')
    logger.info('building.....')

    # 调用 service 模块
    from app.service.build_meta_knowledge_service import MetaKnowledgeService

    async with meta_mysql_client_manager.session_factory() as session1, dw_mysql_client_manager.session_factory() as session2:
        # 创建 service 实例
        meta_knowledge_service = MetaKnowledgeService(
            meta_mysql_repository = MetaMysqlRepository(session1),
            dw_mysql_repository = DwMysqlRepository(session2),
            column_qdrant_repository = ColumnQdrantRepository(qdrant_client_manager.client),
            metric_qdrant_repository = MetricQdrantRepository(qdrant_client_manager.client),
            embedding_client = embedding_client_manager.client,
            value_es_repository= ValueEsRepository(es_client_manager.client)
        )
        await meta_knowledge_service.build(conf_path)

    await meta_mysql_client_manager.close()
    await dw_mysql_client_manager.close()
    await qdrant_client_manager.close()
    await es_client_manager.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build meta knowledge',
        prog='liutianba7'
    )
    parser.add_argument('--config', '-c', help='config file path')
    args = parser.parse_args()
    if args.config is None:
        logger.error('请指定配置文件路径')
        sys.exit(1)

    asyncio.run(build(Path(args.config)))
