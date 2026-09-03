"""
STEP 4 · 리뷰 분석 (리뷰 → 상품)

reviews 컬렉션(STEP3에서 status=PENDING으로 저장된 제품별 리뷰)을 처리해 상품 단위로 집계한
products 컬렉션을 만든다. 상품의 정체성(identity)은 Amazon ASIN이다.

    1) 품질 판정: specificity·relevance·skin_info 3축이 모두 QUALITY_MIN_SCORE 이상이 아니면
       DISQUALIFIED로 표시하고 제외한다.
    2) Amazon 해결(resolution): 품질 통과 리뷰를 원문 (product_name, brand)로 묶어, 고유 조합당
       Amazon SearchItems 상위 10개를 받아 Gemini가 "동일 상품"을 판정한다(amazon_client).
       - 찾으면: 그 조합의 모든 리뷰에 asin을 달고 COMPLETED로 표시.
       - Amazon에 없으면(-1/결과없음): NOTFOUND로 표시하고 제외한다.
       - 같은 원문 표기가 예전에 해결된 적 있으면(reviews 캐시) Amazon을 재호출하지 않고 asin을 재사용.
    3) 저장(products, asin 기준): 각 그룹을 해결하는 즉시 그 asin의 상품을 upsert한다.
       mention_count/ad_ratio는 "그 asin의 COMPLETED 리뷰"에서 video_id 중복제거 후 재계산하므로
       멱등하다 — 중간에 크래시/재시작해도 유실이나 중복 없이 이어서 완성된다.
       신규 asin은 Amazon enrich(name/brand/category/price/description/썸네일/제휴링크)를 함께 저장하고
       썸네일을 product_pictures/{asin}.jpg로 받는다. status=PENDING으로 시작해 step5/6이 채운다.

reviews.status 값: PENDING → DISQUALIFIED / NOTFOUND / COMPLETED (모두 최종 상태)
"""
import asyncio
import datetime
import re

from steps.amazon_client import AmazonClient, download_image, resolve
from steps.db import get_collection
from loguru import logger

# quality_passed 판정 기준: 3축 점수(1~5) 모두 이 값 이상이어야 통과. (튜닝 파라미터)
QUALITY_MIN_SCORE = 3


def _get_reviews_collection():
    return get_collection("reviews")


def _get_products_collection():
    return get_collection("products")


async def ensure_indexes() -> None:
    products_col = _get_products_collection()
    # 상품 정체성은 asin. asin이 있는 문서에 한해 unique.
    await products_col.create_index(
        "asin", unique=True, partialFilterExpression={"asin": {"$exists": True}}
    )
    try:
        await products_col.drop_index("name_1")  # 과거 name unique 인덱스 제거
    except Exception:
        pass

    reviews_col = _get_reviews_collection()
    await reviews_col.create_index("status")
    logger.info("[STEP4] 인덱스 확인 완료 (products.asin unique / reviews.status).")


def _normalize_field(text: str) -> str:
    return re.sub(r"\s+", "", (text or "").strip().lower())


def _raw_key(review: dict) -> tuple[str, str]:
    return (_normalize_field(review.get("product_name")), _normalize_field(review.get("brand")))


async def _disqualify_low_quality() -> int:
    """품질 미달 PENDING 리뷰를 서버측에서 한 번에 DISQUALIFIED로 표시하고, 처리한 개수를 반환한다.
    3축(specificity/relevance/skin_info)이 모두 QUALITY_MIN_SCORE 이상인 경우만 통과($nor로 그 여집합을 제외).
    값이 없는 축도 통과 조건을 못 만족하므로 미달로 간주된다."""
    col = _get_reviews_collection()
    q = QUALITY_MIN_SCORE
    result = await col.update_many(
        {
            "status": "PENDING",
            "$nor": [{"specificity": {"$gte": q}, "relevance": {"$gte": q}, "skin_info": {"$gte": q}}],
        },
        {"$set": {"status": "DISQUALIFIED"}},
    )
    return result.modified_count


async def _build_resolved_cache() -> dict[tuple[str, str], str]:
    """이미 COMPLETED된 리뷰들에서 (원문 name, brand) → asin 캐시를 만든다.
    같은 원문 표기가 다시 들어오면 Amazon을 재호출하지 않기 위함."""
    col = _get_reviews_collection()
    cache: dict[tuple[str, str], str] = {}
    async for r in col.find({"status": "COMPLETED", "asin": {"$exists": True}},
                            {"product_name": 1, "brand": 1, "asin": 1}):
        cache.setdefault(_raw_key(r), r["asin"])
    return cache


async def _upsert_product(asin: str, enrich: dict | None, now: str) -> None:
    """asin의 COMPLETED 리뷰에서 (video_id 중복제거) 카운트를 재계산해 products에 upsert한다.
    재계산 기반이라 멱등 — 같은 asin을 여러 번 저장해도 결과가 같다.
    신규 asin은 enrich(+썸네일)까지 저장하고, 기존 asin은 카운트만 갱신한다."""
    reviews_col = _get_reviews_collection()
    products_col = _get_products_collection()

    per_video: dict[str, float] = {}
    async for r in reviews_col.find({"asin": asin, "status": "COMPLETED"},
                                    {"video_id": 1, "ad_likelihood": 1}):
        per_video[r.get("video_id")] = r.get("ad_likelihood") or 0.0  # 영상당 1회
    mention_count = len(per_video)
    ad_sum = round(sum(per_video.values()), 3)
    ad_ratio = round(ad_sum / mention_count, 3) if mention_count else 0.0
    base = {"mention_count": mention_count, "ad_likelihood_sum": ad_sum,
            "ad_ratio": ad_ratio, "analyzed_at": now}

    existing = await products_col.find_one({"asin": asin}, {"_id": 1})
    if existing is None:
        if not enrich:
            logger.error(f"[STEP4] 신규 asin={asin} enrich 없음 — 상품 생성 건너뜀")
            return
        thumb = await asyncio.to_thread(download_image, enrich.get("image_url"), asin)
        await products_col.update_one(
            {"asin": asin},
            {
                "$set": {
                    **base,
                    "name": enrich.get("name"),
                    "brand": enrich.get("brand"),
                    "category": enrich.get("category"),   # Amazon 7종
                    "price": enrich.get("price"),
                    "description": enrich.get("description"),
                    "thumbnail_url": thumb,
                    "product_url": enrich.get("product_url"),  # 제휴링크
                },
                "$setOnInsert": {"ingredients": None, "inciapi_raw_data": None, "status": "PENDING"},
            },
            upsert=True,
        )
    else:
        # 기존 상품: 카운트만 갱신 (enrich 재요청/이미지 재다운로드 없음).
        await products_col.update_one({"asin": asin}, {"$set": base})


async def _resolve_and_save(qualified_reviews: list[dict], client: AmazonClient,
                            cache: dict[tuple[str, str], str]) -> set[str]:
    """원문 (name, brand) 그룹별로 asin을 해결하고, 해결 즉시 상품을 저장한다(멱등).
    캐시에 있으면 Amazon 호출을 생략한다. 반환: 이번에 저장/갱신한 asin 집합."""
    reviews_col = _get_reviews_collection()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    groups: dict[tuple[str, str], list[dict]] = {}
    for review in qualified_reviews:
        groups.setdefault(_raw_key(review), []).append(review)

    saved: set[str] = set()
    for raw_key, group_reviews in groups.items():
        sample = group_reviews[0]

        if raw_key in cache:
            asin, enrich = cache[raw_key], None  # 기존 상품 — Amazon 재호출 없음
        else:
            enrich = await asyncio.to_thread(resolve, client, sample["product_name"])
            if not enrich or not enrich.get("asin"):
                for review in group_reviews:
                    await reviews_col.update_one({"_id": review["_id"]}, {"$set": {"status": "NOTFOUND"}})
                logger.info(f"[STEP4][NOTFOUND] Amazon에 없음: {sample['product_name']!r} ({len(group_reviews)}개 리뷰)")
                continue
            asin = enrich["asin"]
            cache[raw_key] = asin

        for review in group_reviews:
            await reviews_col.update_one({"_id": review["_id"]}, {"$set": {"asin": asin, "status": "COMPLETED"}})
        await _upsert_product(asin, enrich, now)  # 해결 즉시 저장 → 크래시/재시작에 안전
        saved.add(asin)
        logger.info(f"[STEP4][COMPLETED] asin={asin} ({len(group_reviews)}개 리뷰)")

    return saved


async def run() -> list[str]:
    await ensure_indexes()

    reviews_col = _get_reviews_collection()
    if await reviews_col.count_documents({"status": "PENDING"}) == 0:
        logger.info("[STEP4] 처리할 PENDING 리뷰가 없습니다.")
        return []

    # ① 쓰기: 품질 미달 리뷰를 서버측에서 한 번에 DISQUALIFIED 처리.
    disqualified = await _disqualify_low_quality()
    # ② 읽기: 남은 PENDING(= 품질 통과)만 가져온다.
    qualified_reviews = [doc async for doc in reviews_col.find({"status": "PENDING"})]
    logger.info(
        f"[STEP4] 품질 미달 {disqualified}개 제외, {len(qualified_reviews)}개 Amazon 해결 대상."
    )
    if not qualified_reviews:
        logger.info("[STEP4] 품질 통과 리뷰가 없습니다.")
        return []

    cache = await _build_resolved_cache()
    client = AmazonClient()
    saved = await _resolve_and_save(qualified_reviews, client, cache)
    logger.info(f"[STEP4] {len(saved)}개 상품 저장/갱신 완료.")
    return list(saved)


if __name__ == "__main__":
    asyncio.run(run())
