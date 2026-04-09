import uuid
from dataclasses import asdict
from pathlib import Path

from langchain_huggingface import HuggingFaceEndpointEmbeddings

from app.conf.config_loader import load_config
from app.conf.meta_config import MetaConfig, TableConfig
from app.core.log import logger
from app.entities.column_info import ColumnInfo
from app.entities.column_metric import ColumnMetric
from app.entities.metric_info import MetricInfo
from app.entities.table_info import TableInfo
from app.entities.value_info import ValueInfo
from app.repository.es.value_es_repository import ValueEsRepository
from app.repository.mysql.dw.dw_mysql_repository import DwMysqlRepository
from app.repository.mysql.meta.meta_mysql_repository import MetaMysqlRepository
from app.repository.qdrant.column_qdrant_repository import ColumnQdrantRepository
from qdrant_client.models import PointStruct

from app.repository.qdrant.metric_qdrant_repository import MetricQdrantRepository


class MetaKnowledgeService:
    def __init__(self,
                 meta_mysql_repository: MetaMysqlRepository,
                 dw_mysql_repository: DwMysqlRepository,
                 column_qdrant_repository: ColumnQdrantRepository,
                 metric_qdrant_repository: MetricQdrantRepository,
                 embedding_client: HuggingFaceEndpointEmbeddings,
                 value_es_repository: ValueEsRepository
                 ):
        self.meta_mysql_repository = meta_mysql_repository
        self.dw_mysql_repository = dw_mysql_repository
        self.column_qdrant_repository = column_qdrant_repository
        self.metric_qdrant_repository = metric_qdrant_repository
        self.embedding_client = embedding_client
        self.value_es_repository = value_es_repository

    async def build(self, meta_conf_path: Path):
        if not meta_conf_path.exists():
            logger.error('配置文件不存在')
            raise Exception('配置文件不存在')

        meta_conf = load_config(meta_conf_path, MetaConfig)
        logger.info(f'开始构建元知识，配置文件: {meta_conf_path}')

        if not meta_conf.tables:
            return

        if meta_conf.tables:
            # 2.1 同步表信息到 MySQL
            table_infos, column_infos = await self._build_table_and_column_infos(meta_conf.tables)
            await self._save_to_mysql(table_infos, column_infos)
            logger.success(f'同步表信息到 MySQL 成功，共 {len(table_infos)} 张表，{len(column_infos)} 个字段')

            # 2.2 同步字段信息到 Qdrant（向量检索）
            await self._sync_columns_to_qdrant(column_infos)
            logger.success('同步字段信息到 Qdrant 成功')

            # 2.3 同步字段取值到 ES（全文检索）
            await self._sync_column_values_to_es(meta_conf.tables)
            logger.success('同步字段取值到 ES 成功')

        if meta_conf.metrics:
            # 3 同步指标信息 + 列指标关系

            # 3.1 同步信息到数据库
            metrics, column_metrics = await self._sync_metrics_and_metric_column_to_mysql(meta_conf)
            # 3.2 同步信息到 Qdrant（向量检索）
            await self._sync_metrics_to_qdrant(meta_conf)
            logger.success(f'同步指标信息成功，共 {len(meta_conf.metrics)} 个指标')

        logger.success('元知识构建完成')

    # -------------------- 私有方法 --------------------

    async def   _build_table_and_column_infos(self, tables: list[TableConfig]) -> tuple[list[TableInfo], list[ColumnInfo]]:
        table_infos: list[TableInfo] = []
        column_infos: list[ColumnInfo] = []

        for table in tables:
            table_infos.append(TableInfo(
                id=table.name,
                name=table.name,
                role=table.role,
                description=table.description
            ))

            column_types = await self.dw_mysql_repository.get_column_types(table.name)

            for column in table.columns:
                examples = await self.dw_mysql_repository.get_column_examples(table.name, column.name)
                column_info = ColumnInfo(
                    id=f'{table.name}.{column.name}',
                    name=column.name,
                    type=column_types.get(column.name, 'unknown'),
                    role=column.role,
                    examples=examples,
                    description=column.description,
                    alias=column.alias,
                    table_id=table.name
                )
                logger.info(f'{column_info.name}, {column_info.type}, {column_info.examples}')
                column_infos.append(column_info)

        return table_infos, column_infos

    async def _save_to_mysql(self, table_infos: list[TableInfo], column_infos: list[ColumnInfo]):
        async with self.meta_mysql_repository.session.begin():
            self.meta_mysql_repository.batch_save_tables(table_infos)
            self.meta_mysql_repository.batch_save_columns(column_infos)

    async def _sync_columns_to_qdrant(self, column_infos: list[ColumnInfo]):
        await self.column_qdrant_repository.create_collection()

        points: list[dict] = []
        for column_info in column_infos:
            points.append({
                'id': uuid.uuid4().hex,
                'embedding_text': column_info.name,
                'payload': asdict(column_info)
            })
            points.append({
                'id': uuid.uuid4().hex,
                'embedding_text': column_info.description,
                'payload': asdict(column_info)
            })
            for alis in column_info.alias:
                points.append({
                    'id': uuid.uuid4().hex,
                    'embedding_text': alis,
                    'payload': asdict(column_info)
                })

        embedding_texts = [point['embedding_text'] for point in points]
        embeddings = []
        for i in range(0, len(embedding_texts), 32):
            embeddings.extend(await self.embedding_client.aembed_documents(embedding_texts[i:i + 32]))

        for i in range(len(points)):
            points[i]['vector'] = embeddings[i]

        await self.column_qdrant_repository.upsert(points)

    async def _sync_column_values_to_es(self, tables: list[TableConfig]):
        await self.value_es_repository.create_index()

        values: list[ValueInfo] = []
        for table in tables:
            for column in table.columns:
                if column.sync:
                    resp = await self.dw_mysql_repository.get_column_values(table.name, column.name)
                    for value in resp:
                        values.append(ValueInfo(
                            id=f'{table.name}.{column.name}.{value}',
                            value=value,
                            column_id=f'{table.name}.{column.name}'
                        ))

        await self.value_es_repository.bulk(values, 64)

    async def _sync_metrics_and_metric_column_to_mysql(self, meta_conf: MetaConfig):
        # 同步指标信息到 MySQL
        # 构建指标信息
        metrics: list[MetricInfo] = []
        for metric in meta_conf.metrics:
            metrics.append(
                MetricInfo(
                    id=metric.name,
                    name=metric.name,
                    description=metric.description,
                    relevant_columns=metric.relevant_columns,
                    alias=metric.alias
                )
            )

        # 构建列指标关系
        column_metrics: list[ColumnMetric] = []
        for metric in meta_conf.metrics:
            for column in metric.relevant_columns:
                column_metrics.append(
                    ColumnMetric(
                        column_id=column,
                        metric_id=metric.name
                    )
                )

        # 保存到 MySQL
        async with self.meta_mysql_repository.session.begin():
            self.meta_mysql_repository.batch_save_metrics(metrics)
            self.meta_mysql_repository.batch_save_column_metrics(column_metrics)

        logger.success('同步指标信息成功')
        return metrics, column_metrics

    async def _sync_metrics_to_qdrant(self, meta_conf: MetaConfig):
        # 确保 collection 创建
        await self.metric_qdrant_repository.create_collection()

        # 收集数据
        points: list[dict] = []
        for metric_info in meta_conf.metrics:
            points.append({
                'id': uuid.uuid4().hex,
                'embedding_text': metric_info.name,
                'payload': asdict(metric_info)
            })
            points.append({
                'id': uuid.uuid4().hex,
                'embedding_text': metric_info.description,
                'payload': asdict(metric_info)
            })
            for alis in metric_info.alias:
                points.append({
                    'id': uuid.uuid4().hex,
                    'embedding_text': alis,
                    'payload': asdict(metric_info)
                })

        embedding_texts = [point['embedding_text'] for point in points]
        embeddings = []
        for i in range(0, len(embedding_texts), 32):
            embeddings.extend(await self.embedding_client.aembed_documents(embedding_texts[i:i + 32]))

        for i in range(len(points)):
            points[i]['vector'] = embeddings[i]

        await self.metric_qdrant_repository.upsert(points)
