"""
step7: products_summarized 컬렉션의 11개 피부 고민 점수를 전체 제품 기준으로
min-max 정규화하여 {concern}_norm 필드(0.0~1.0)로 저장합니다.

정규화 대상 필드:
    dark_circle, wrinkle, oily, pore, blackhead, acne,
    sensitivity, melanin, hydration, rough, red_spot
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
_summarized_col = None


def _get_col():
    global _client, _summarized_col
    if _client is None:
        _client = AsyncIOMotorClient(MONGODB_URI)
    _summarized_col = _client[MONGODB_DB_NAME]["products_summarized"]
    return _summarized_col


async def run() -> None:
    col = _get_col()

    docs = []
    async for doc in col.find({}, {"_id": 1, "product_name": 1, **{f: 1 for f in CONCERN_FIELDS}}):
        docs.append(doc)

    if not docs:
        logger.warning("[STEP7] products_summarized 컬렉션이 비어 있습니다.")
        return

    # 각 고민별 min/max 계산
    min_max: dict[str, tuple[float, float]] = {}
    for field in CONCERN_FIELDS:
        values = [d[field] for d in docs if field in d and d[field] is not None]
        if not values:
            min_max[field] = (0.0, 1.0)
            continue
        lo, hi = min(values), max(values)
        min_max[field] = (lo, hi)

    logger.info("[STEP7] min/max per concern:")
    for field, (lo, hi) in min_max.items():
        logger.info(f"  {field}: [{lo:.4f}, {hi:.4f}]")

    # 각 제품에 _norm 필드 업데이트
    updated = 0
    for doc in docs:
        norm_fields: dict[str, float] = {}
        for field in CONCERN_FIELDS:
            raw = doc.get(field)
            if raw is None:
                norm_fields[f"{field}_norm"] = None
                continue
            lo, hi = min_max[field]
            if hi == lo:
                norm_fields[f"{field}_norm"] = 0.0
            else:
                norm_fields[f"{field}_norm"] = round((raw - lo) / (hi - lo), 4)

        await col.update_one({"_id": doc["_id"]}, {"$set": norm_fields})
        updated += 1

    logger.info(f"[STEP7] Done. Normalized scores written to {updated} products.")


if __name__ == "__main__":
    asyncio.run(run())
