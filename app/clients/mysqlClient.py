from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.sql import text
from app.conf.app_config import DBConfig, app_config
import asyncio

class MysqlClientManager:
    def __init__(self, config: DBConfig):
        self.engine: AsyncEngine | None = None
        self.config: DBConfig = config
        self.session_factory: async_sessionmaker | None = None

    def _get_url(self):
        return f'mysql+asyncmy://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}?charset=utf8mb4'

    def init(self):
        self.engine = create_async_engine(
            self._get_url(),
            pool_pre_ping=True
        )
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
            autoflush=True,

        )

    async def close(self):
        await self.engine.dispose()


meta_mysql_client_manager = MysqlClientManager(app_config.db_meta)
dw_mysql_client_manager = MysqlClientManager(app_config.db_dw)

meta_mysql_client_manager.init()
dw_mysql_client_manager.init()

if __name__ == '__main__':
    async def test_connection():
        meta_mysql_client_manager.init()
        session_factory = meta_mysql_client_manager.session_factory
        async with session_factory() as session:
            stmt = text("select 1")
            result = await session.execute(stmt)
            print(result.fetchone())

        await meta_mysql_client_manager.close()


    asyncio.run(test_connection())
