"""
MySQL 客户端
"""
import asyncio

from app.conf.app_config import DBConfig, app_config
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy import text

# MySQL 客户端管理器
class MySQLClientManager():

    def __init__(self, config: DBConfig):
        self.engine: AsyncEngine | None = None
        self.config = config

    def _get_url(self):
        return f"mysql+asyncmy://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}?charset=utf8mb4"

    def init(self):
        self.engine = create_async_engine(self._get_url())

    async def close(self):
        if self.engine:
            await self.engine.dispose()

# MySQL 客户端管理器实例 单例
dw_client_manager = MySQLClientManager(app_config.db_dw)
meta_client_manager = MySQLClientManager(app_config.db_meta)

if __name__ == '__main__':
    async def my_test():
        dw_client_manager.init()
        engine = dw_client_manager.engine

        async with AsyncSession(engine) as session:
            sql = "select * from dim_customer limit 10"

            resp = await session.execute(text(sql))
            rows = resp.fetchall()
            print(rows)


    asyncio.run(my_test())
