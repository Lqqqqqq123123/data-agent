from sqlalchemy.ext.asyncio import AsyncSession

from app.entities.table_info import TableInfo
from app.entities.column_info import ColumnInfo
from app.entities.metric_info import MetricInfo
from app.entities.column_metric import ColumnMetric
from app.models.table_info_mysql import TableInfoMySQL
from app.models.column_info_mysql import ColumnInfoMySQL
from app.models.metric_info_mysql import MetricInfoMySQL
from app.models.column_metric_mysql import ColumnMetricMySQL


class MetaMysqlRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    def batch_save_tables(self, table_infos: list[TableInfo]):
        orm_objects = [
            TableInfoMySQL(
                id=t.id,
                name=t.name,
                role=t.role,
                description=t.description
            )
            for t in table_infos
        ]
        self.session.add_all(orm_objects)

    def batch_save_columns(self, column_infos: list[ColumnInfo]):
        orm_objects = [
            ColumnInfoMySQL(
                id=c.id,
                name=c.name,
                type=c.type,
                role=c.role,
                examples=c.examples,
                description=c.description,
                alias=c.alias,
                table_id=c.table_id
            )
            for c in column_infos
        ]
        self.session.add_all(orm_objects)

    def batch_save_metrics(self, metric_infos: list[MetricInfo]):
        orm_objects = [
            MetricInfoMySQL(
                id=m.id,
                name=m.name,
                description=m.description,
                relevant_columns=m.relevant_columns,
                alias=m.alias
            )
            for m in metric_infos
        ]
        self.session.add_all(orm_objects)

    def batch_save_column_metrics(self, column_metrics: list[ColumnMetric]):
        orm_objects = [
            ColumnMetricMySQL(
                column_id=cm.column_id,
                metric_id=cm.metric_id
            )
            for cm in column_metrics
        ]
        self.session.add_all(orm_objects)
