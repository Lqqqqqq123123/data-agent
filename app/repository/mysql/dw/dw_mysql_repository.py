from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text


class DwMysqlRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_column_types(self, table_name: str) -> dict[str, str]:
        """获取指定表的所有字段类型，返回 {field_name: type} 映射"""
        stmt = text(f"show columns from {table_name}")
        result = (await self.session.execute(stmt)).mappings().all()
        return {row['Field']: row['Type'] for row in result}

    async def get_column_examples(self, table_name: str, column_name: str, limit: int = 10) -> list:
        """获取指定表指定字段的取值示例"""
        stmt = text(f"select distinct {column_name} from {table_name} limit {limit}")
        result = (await self.session.execute(stmt)).mappings().all()
        return [row[column_name] for row in result]


    async def get_column_values(self, table_name: str, column_name: str) -> list:
        """获取指定表指定字段的取值"""
        stmt = text(f"select distinct {column_name} from {table_name}")
        result = (await self.session.execute(stmt)).mappings().all()
        return [row[column_name] for row in result]