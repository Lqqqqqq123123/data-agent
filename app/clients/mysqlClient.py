from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy.sql import text
import asyncio

from app.conf.app_config import DBConfig, app_config
class MysqlClientManager:
    def __init__(self, config:DBConfig):
        self.engine:AsyncEngine | None =  None
        self.config:DBConfig = config

    def _get_url(self):
        return f'mysql+asyncmy://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}?charset=utf8mb4'

    def init(self):
        self.engine = create_async_engine(
            self._get_url(),
            pool_pre_ping=True
        )

    async def close(self):
        await self.engine.dispose()

meta_mysql_client_manager = MysqlClientManager(app_config.db_meta)
dw_mysql_client_manager = MysqlClientManager(app_config.db_dw)


if __name__ == '__main__':

    async def test_connection():
        meta_mysql_client_manager.init()
        engine = meta_mysql_client_manager.engine

        async with AsyncSession(engine, autoflash=True, expire_on_commit=False) as session:
            result = await session.execute(text("select 1"))
            print(result.fetchone())

    asyncio.run(test_connection())

