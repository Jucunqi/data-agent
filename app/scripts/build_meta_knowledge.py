import argparse
import asyncio
from pathlib import Path
from app.repositories.mysql.dw.dw_mysql_repository import DwMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.services.meta_knowledge_service import MetaKnowledgeService
from app.clients.mysql_client_manager import dw_client_manager, meta_client_manager


async def build(config_path: Path):
    meta_client_manager.init()
    dw_client_manager.init()
    async with (
        meta_client_manager.session_factory() as session,
        dw_client_manager.session_factory() as dw_session,
    ):
        meta_mysql_repository = MetaMySQLRepository(session)
        dw_mysql_repository = DwMySQLRepository(dw_session)
        meta_knowledge_service = MetaKnowledgeService(
            meta_mysql_repository, dw_mysql_repository
        )
        await meta_knowledge_service.build(config_path)

    await meta_client_manager.close()
    await dw_client_manager.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--conf")  # 接受一个值的选项

    args = parser.parse_args()
    config_path = args.conf

    asyncio.run(build(Path(config_path)))
