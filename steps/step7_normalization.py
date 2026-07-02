"""
step7: products_summarized 컬렉션의 11개 피부 고민 점수를 전체 제품 기준으로
min-max 정규화(0~100)하여 products_normalized 컬렉션에 저장합니다.
"""
import asyncio
import os

from dotenv import load_dotenv
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")

CONCERN_FIELDS = [
    "dark_circle", "wrinkle", "oily", "pore", "blackhead",
    "acne", "sensitivity", "melanin", "hydration", "rough", "red_spot",
]

_client: AsyncIOMotorClient | None = None


def _get_cols():
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(MONGODB_URI)
    db = _client[MONGODB_DB_NAME]
    return db["products_summarized"], db["products_normalized"]


async def run() -> None:
    src_col, dst_col = _get_cols()
    await dst_col.create_index("product_name", unique=True)

    projection = {"_id": 0, "product_name": 1, "brand": 1, "category": 1,
                  **{f: 1 for f in CONCERN_FIELDS}}

    docs = []
    async for doc in src_col.find({}, projection):
        docs.append(doc)

    if not docs:
        logger.warning("[STEP7] products_summarized 컬렉션이 비어 있습니다.")
        return

    # 각 고민별 min/max 계산
    min_max: dict[str, tuple[float, float]] = {}
    for field in CONCERN_FIELDS:
        values = [d[field] for d in docs if d.get(field) is not None]
        lo, hi = (min(values), max(values)) if values else (0.0, 1.0)
        min_max[field] = (lo, hi)

    logger.info("[STEP7] 정규화 범위 (min → max):")
    for field in CONCERN_FIELDS:
        lo, hi = min_max[field]
        logger.info(f"  {field}: [{lo:.4f}, {hi:.4f}]")

    # 정규화 후 products_normalized에 upsert
    for doc in docs:
        normalized: dict = {
            "product_name": doc.get("product_name"),
            "brand":        doc.get("brand"),
            "category":     doc.get("category"),
        }
        for field in CONCERN_FIELDS:
            raw = doc.get(field)
            if raw is None:
                normalized[field] = None
                continue
            lo, hi = min_max[field]
            normalized[field] = 0 if hi == lo else round((raw - lo) / (hi - lo) * 100, 2)

        await dst_col.update_one(
            {"product_name": normalized["product_name"]},
            {"$set": normalized},
            upsert=True,
        )

    logger.info(f"[STEP7] Done. {len(docs)}개 제품 → products_normalized 저장 완료.")


if __name__ == "__main__":
    asyncio.run(run())
