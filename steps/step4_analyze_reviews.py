import asyncio
import datetime
import os

from dotenv import load_dotenv
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")

_client: AsyncIOMotorClient | None = None
_reviews_col = None
_products_col = None


def _get_reviews_collection():
    global _client, _reviews_col
    if _reviews_col is None:
        _client = AsyncIOMotorClient(MONGODB_URI)
        _reviews_col = _client[MONGODB_DB_NAME]["reviews"]
    return _reviews_col


def _get_products_collection():
    global _client, _products_col
    if _products_col is None:
        if _client is None:
            _client = AsyncIOMotorClient(MONGODB_URI)
        _products_col = _client[MONGODB_DB_NAME]["products"]
    return _products_col


async def ensure_indexes() -> None:
    col = _get_products_collection()
    await col.create_index("product_name", unique=True)
    logger.info("[STEP4] Ensured unique index on products.product_name.")


# reviews 컬렉션에서 필요한 필드만 읽어옵니다.
async def fetch_reviews() -> list[dict]:
    col = _get_reviews_collection()
    projection = {
        "_id": 0,
        "product_name": 1,
        "video_id": 1,
        "analyzed_at": 1,
        "brand": 1,
        "category": 1,
        "channel_name": 1,
        "summary": 1,
        "is_ad": 1,
        "ad_confidence": 1,
    }
    reviews = []
    async for doc in col.find({"product_name": {"$ne": None}}, projection):
        reviews.append(doc)
    logger.info(f"[STEP4] Fetched {len(reviews)} reviews from DB.")
    return reviews


# 광고 여부에 따라 가중치를 부여합니다.
# 순수 리뷰(is_ad=False)는 1.0, 광고(is_ad=True)는 0.3으로 계산합니다.
# ad_confidence가 높을수록 추가 패널티를 적용합니다.
def _calc_weight(is_ad: bool, ad_confidence: float) -> float:
    if is_ad:
        return max(0.1, 0.3 - ad_confidence * 0.2)
    return max(0.3, 1.0 - ad_confidence * 0.5)


# 리뷰 목록을 제품별로 집계하고 분석합니다.
def aggregate_by_product(reviews: list[dict]) -> list[dict]:
    product_map: dict[str, dict] = {}

    for review in reviews:
        name = review.get("product_name")
        if not name:
            continue

        if name not in product_map:
            product_map[name] = {
                "product_name": name,
                "brand": review.get("brand"),
                "category": review.get("category"),
                "mention_count": 0,
                "ad_mention_count": 0,
                "ad_weighted_score": 0.0,
                "video_ids": [],
            }

        entry = product_map[name]
        entry["mention_count"] += 1

        is_ad = bool(review.get("is_ad", False))
        ad_confidence = float(review.get("ad_confidence") or 0.0)

        if is_ad:
            entry["ad_mention_count"] += 1

        entry["ad_weighted_score"] += _calc_weight(is_ad, ad_confidence)

        video_id = review.get("video_id")
        if video_id and video_id not in entry["video_ids"]:
            entry["video_ids"].append(video_id)

    results = []
    for entry in product_map.values():
        mention_count = entry["mention_count"]
        ad_count = entry["ad_mention_count"]
        entry["ad_ratio"] = round(ad_count / mention_count, 3) if mention_count > 0 else 0.0
        entry["ad_weighted_score"] = round(entry["ad_weighted_score"], 3)
        results.append(entry)

    results.sort(key=lambda x: x["ad_weighted_score"], reverse=True)
    return results


async def save_products(products: list[dict]) -> None:
    col = _get_products_collection()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for product in products:
        record = {**product, "analyzed_at": now}
        await col.update_one(
            {"product_name": record["product_name"]},
            {"$set": record},
            upsert=True,
        )
    logger.info(f"[STEP4] Saved {len(products)} products to DB.")


async def run() -> list[dict]:
    await ensure_indexes()
    reviews = await fetch_reviews()
    if not reviews:
        logger.warning("[STEP4] No reviews found.")
        return []

    products = aggregate_by_product(reviews)
    logger.info(f"[STEP4] {len(reviews)} reviews → {len(products)} unique products.")
    await save_products(products)
    return products


if __name__ == "__main__":
    asyncio.run(run())
