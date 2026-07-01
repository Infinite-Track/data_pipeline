import datetime
import os
from typing import Any

from dotenv import load_dotenv
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")

if not MONGODB_URI:
    logger.warning("MONGODB_URI not set in environment.")
if not MONGODB_DB_NAME:
    logger.warning("MONGODB_DB_NAME not set in environment.")

_client: AsyncIOMotorClient | None = None
_collection = None


# MongoDB 컬렉션을 초기화하고 캐시합니다.
# 최초 호출 시에만 AsyncIOMotorClient를 생성합니다.
def _get_collection():
    global _client, _collection
    if _collection is None:
        if not MONGODB_URI or not MONGODB_DB_NAME:
            raise RuntimeError("MongoDB configuration is missing.")
        _client = AsyncIOMotorClient(MONGODB_URI)
        db = _client[MONGODB_DB_NAME]
        _collection = db["reviews"]
    return _collection


# video_id 단독 유니크 인덱스를 삭제하고 (video_id, product_name) 복합 인덱스로 교체합니다.
# 구조 변경(영상 1개 → 제품 N개)으로 인해 기존 인덱스와 충돌하기 때문입니다.
async def ensure_indexes() -> None:
    collection = _get_collection()
    try:
        await collection.drop_index("video_id_1")
        logger.info("[DB] Dropped old video_id_1 unique index.")
    except Exception:
        pass
    await collection.create_index([("video_id", 1), ("product_name", 1)], unique=True)
    logger.info("[DB] Ensured compound index on (video_id, product_name).")


# video_id로 문서 존재 여부를 확인합니다.
# 이미 처리된 영상은 중복 저장을 피하기 위해 건너뜁니다.
async def already_exists(video_id: str) -> bool:
    collection = _get_collection()
    document = await collection.find_one({"video_id": video_id}, {"_id": 1})
    return document is not None


# 리뷰 데이터를 MongoDB에 upsert 방식으로 저장합니다.
# video_id를 기준으로 문서를 갱신하거나 없으면 새로 생성합니다.
async def save(document: dict[str, Any]) -> None:
    collection = _get_collection()
    if "video_id" not in document:
        raise ValueError("Document must include video_id for upsert.")

    record = document.copy()
    record["source"] = record.get("source", "youtube")
    record["analyzed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    await collection.update_one(
        {"video_id": record["video_id"], "product_name": record.get("product_name")},
        {"$set": record},
        upsert=True,
    )
    logger.info(f"[SAVE] Saved review for {record['video_id']} - {record.get('product_name')}")
