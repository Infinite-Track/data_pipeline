"""MongoDB 연결 헬퍼.

프로세스당 AsyncIOMotorClient를 1개만 만들어 재사용한다.
추후 Prefect task로 전환해도 각 task가 같은 이벤트 루프 안에서 get_collection()을
호출하면 커넥션이 재사용되므로 구조를 바꿀 필요가 없다.
"""
import os

from dotenv import load_dotenv
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")

if not MONGODB_URI:
    logger.warning("MONGODB_URI가 설정되지 않았습니다.")
if not MONGODB_DB_NAME:
    logger.warning("MONGODB_DB_NAME이 설정되지 않았습니다.")

_client: AsyncIOMotorClient | None = None


def get_collection(name: str) -> AsyncIOMotorCollection:
    """이름으로 컬렉션 핸들을 반환한다. (youtubers / videos / raw_reviews / reviews / products / products_summarized / products_normalized)"""
    global _client
    if _client is None:
        if not MONGODB_URI or not MONGODB_DB_NAME:
            raise RuntimeError("MongoDB 설정(MONGODB_URI/MONGODB_DB_NAME)이 없습니다.")
        _client = AsyncIOMotorClient(MONGODB_URI)
    return _client[MONGODB_DB_NAME][name]
