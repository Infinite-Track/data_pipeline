"""
STEP 4 · 리뷰 분석 (리뷰 → 상품)

reviews 컬렉션(검증까지 끝난 제품별 리뷰)을 상품 단위로 집계해 products 컬렉션을 만든다.

    1) 조건 평가: specificity·relevance·skin_info를 종합해 quality_passed 계산.
       quality_passed=False인 리뷰는 집계에서 제외한다 (원본은 reviews에 그대로 남는다).
    2) 상품명 1차 그룹핑: 표기 변화(대소문자, 공백)를 표준화해 동일 상품을 통합한다.
    3) 상품명 통일 + Amazon 판매 여부 확인: 신규 상품이면 Gemini + Google 검색 그라운딩으로
       Amazon Global(amazon.com)에서 공식 상품명을 찾는다. 최종 목표가 아마존 제휴링크로
       소비자를 유도하는 것이라, Amazon에서 찾지 못한 신규 상품은 애초에 추천 대상이 될 수
       없으므로 통째로 제외한다 (기존에 이미 저장된 상품은 재검색하지 않고 그대로 유지).
    4) 최종 저장 형식 통일: product_name/brand/category 전부 소문자 + 공백 완전 제거.
    5) 상품 단위 집계: mention_count, ad_ratio 산출.

Amazon Global 검색은 신규 상품에 대해서만 한다. products.raw_key(1차 그룹핑 키, 정규화 전
원본 그룹핑 문자열)를 기준으로 이미 존재하는 상품인지 먼저 확인하고, 이미 있으면 검색을
건너뛰고 name/brand/category는 그대로 둔 채 mention_count/ad_ratio만 갱신한다.
(그렇지 않으면 파이프라인을 돌릴 때마다 같은 상품을 계속 재검색하게 되어 비효율적이다.)

주의: 서로 다른 raw_key를 가진 두 그룹이 Amazon 검색 결과 같은 공식 상품명으로 수렴하는
경우, products 문서가 2개로 남을 수 있다 (raw_key만 유니크 인덱스이고 name은 아님).
흔한 케이스는 아니라 지금 단계에서는 별도 병합 로직 없이 알려진 한계로 남겨둔다.

결과는 products 컬렉션에 저장하며, 성분(ingredients)·INCI 분석(inciapi_raw_data)은
아직 없으므로 status=PENDING으로 시작해 step5/6이 채워나간다.
"""
import asyncio
import datetime
import re
from collections import Counter

from loguru import logger

from steps.db import get_collection
from steps.gemini_client import get_client
from steps.llm_utils import parse_json_response

try:
    from google.genai import types as genai_types
except ImportError:
    genai_types = None

GEMINI_MODEL = "gemini-2.5-flash"

# quality_passed 판정 기준: 3축 점수(1~5) 모두 이 값 이상이어야 통과.
# 값 자체는 운영하면서 조정될 수 있는 튜닝 파라미터다.
QUALITY_MIN_SCORE = 3

# 이 값 이상이면 "광고성 언급"으로 간주해 ad_ratio 계산에 반영한다.
AD_LIKELIHOOD_THRESHOLD = 0.5


def _get_reviews_collection():
    return get_collection("reviews")


def _get_products_collection():
    return get_collection("products")


async def ensure_indexes() -> None:
    col = _get_products_collection()
    # name이 아니라 raw_key를 유니크 키로 쓴다. Amazon 검색으로 name이 나중에 바뀔 수 있어서
    # "이 리뷰 그룹을 이미 처리했는지"를 판단하는 안정적인 키는 raw_key여야 한다.
    await col.create_index("raw_key", unique=True)
    logger.info("[STEP4] products.raw_key 유니크 인덱스 확인 완료.")


def _quality_passed(review: dict) -> bool:
    return (
        review.get("specificity", 0) >= QUALITY_MIN_SCORE
        and review.get("relevance", 0) >= QUALITY_MIN_SCORE
        and review.get("skin_info", 0) >= QUALITY_MIN_SCORE
    )


def _normalize_field(text: str) -> str:
    """소문자로 바꾸고 공백을 전부 제거한다. (product_name/brand/category 최종 저장 형식)"""
    return re.sub(r"\s+", "", text.strip().lower())


def aggregate_by_product(reviews: list[dict]) -> list[dict]:
    """quality_passed 리뷰만 1차로 상품 단위 그룹핑한다. (Amazon 조회 전 단계)"""
    groups: dict[str, dict] = {}

    for review in reviews:
        if not _quality_passed(review):
            continue

        raw_name = review.get("product_name")
        if not raw_name:
            continue
        key = _normalize_field(raw_name)

        if key not in groups:
            groups[key] = {
                "name_variants": Counter(),
                "brand_variants": Counter(),
                "category_variants": Counter(),
                "mention_count": 0,
                "ad_mention_count": 0,
            }

        group = groups[key]
        group["name_variants"][raw_name] += 1
        if review.get("brand"):
            group["brand_variants"][review["brand"]] += 1
        if review.get("category"):
            group["category_variants"][review["category"]] += 1
        group["mention_count"] += 1
        if review.get("ad_likelihood", 0.0) >= AD_LIKELIHOOD_THRESHOLD:
            group["ad_mention_count"] += 1

    results = []
    for key, group in groups.items():
        mention_count = group["mention_count"]
        results.append({
            "raw_key": key,
            # 가장 많이 쓰인 원문 표기를 Amazon 검색용 후보 이름으로 사용
            "candidate_name": group["name_variants"].most_common(1)[0][0],
            "brand": group["brand_variants"].most_common(1)[0][0] if group["brand_variants"] else None,
            "category": group["category_variants"].most_common(1)[0][0] if group["category_variants"] else None,
            "mention_count": mention_count,
            "ad_ratio": round(group["ad_mention_count"] / mention_count, 3) if mention_count else 0.0,
        })

    results.sort(key=lambda p: p["mention_count"], reverse=True)
    return results


def _lookup_amazon_global_name_sync(candidate_name: str, brand: str | None) -> str | None:
    """Google 검색 그라운딩으로 Amazon Global(amazon.com)에서 이 상품의 공식 리스팅 제목을 찾는다.

    찾으면 그 공식 표기로 product_name을 통일해 리뷰마다 다른 표기 차이를 없앤다.
    찾지 못하면 None을 반환하며, 호출부(_resolve_products)는 이 경우 해당 상품을 통째로 제외한다.
    """
    client = get_client()
    if not client or not genai_types:
        return None

    brand_hint = f"Brand: {brand}" if brand else ""
    prompt = (
        f'K-beauty product: "{candidate_name}"\n{brand_hint}\n\n'
        "Search Amazon Global (amazon.com) for this exact product's official listing.\n"
        'Return raw JSON only: {"found": boolean, "amazon_title": string or null}\n'
        "- found: true only if a specific matching product listing was found (not a search results page)\n"
        "- amazon_title: the exact product title as shown on the Amazon listing, or null\n"
    )
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
                temperature=0.1,
            ),
        )
        parsed = parse_json_response(response.text or "")
        if isinstance(parsed, dict) and parsed.get("found") and parsed.get("amazon_title"):
            return str(parsed["amazon_title"]).strip()
    except Exception as exc:
        logger.warning(f"[STEP4] Amazon Global 검색 실패 ({candidate_name!r}): {exc}")
    return None


async def _resolve_products(groups: list[dict]) -> list[dict]:
    """그룹별로 신규 상품이면 Amazon 검색 결과가 있을 때만 채택하고, 기존 상품이면 집계치만 갱신.

    최종적으로 아마존 제휴링크로 소비자를 유도하는 게 목적이라, Amazon Global에서
    찾지 못한 상품은 애초에 추천 대상이 될 수 없다 → 신규 상품인데 못 찾으면 통째로 제외한다.

    주의(알려진 비효율): 제외된 상품은 products에 기록이 안 남으므로, 다음 실행에서도
    raw_key로 "이미 확인함"을 알 수 없어 Amazon 검색을 다시 시도한다. 지금은 Amazon API를
    붙이기 전 단계라 감안하지 않았고, 재검색 낭비가 문제 되면 그때 거절 기록을 추가하면 된다.
    """
    products_col = _get_products_collection()
    resolved = []

    for group in groups:
        existing = await products_col.find_one(
            {"raw_key": group["raw_key"]}, {"_id": 0, "name": 1, "brand": 1, "category": 1}
        )

        if existing:
            name, brand, category = existing["name"], existing.get("brand"), existing.get("category")
        else:
            amazon_title = await asyncio.to_thread(
                _lookup_amazon_global_name_sync, group["candidate_name"], group["brand"]
            )
            if not amazon_title:
                logger.info(f"[STEP4][DROP] Amazon Global에 없는 상품이라 제외: {group['candidate_name']!r}")
                continue

            name = _normalize_field(amazon_title)
            brand = _normalize_field(group["brand"]) if group["brand"] else None
            category = _normalize_field(group["category"]) if group["category"] else None
            logger.info(f"[STEP4] 신규 상품 등록: {name!r} (Amazon 확인됨)")

        resolved.append({
            "raw_key": group["raw_key"],
            "name": name,
            "brand": brand,
            "category": category,
            "mention_count": group["mention_count"],
            "ad_ratio": group["ad_ratio"],
        })

    return resolved


async def _save_products(products: list[dict]) -> None:
    col = _get_products_collection()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for product in products:
        # 이미 존재하는 상품이면 ingredients/inciapi_raw_data/status는 건드리지 않는다.
        # (step5/6이 채운 값을 재집계 시 덮어쓰지 않기 위함)
        await col.update_one(
            {"raw_key": product["raw_key"]},
            {
                "$set": {**product, "analyzed_at": now},
                "$setOnInsert": {"ingredients": None, "inciapi_raw_data": None, "status": "PENDING"},
            },
            upsert=True,
        )
    logger.info(f"[STEP4] {len(products)}개 상품을 products에 저장했습니다.")


async def run() -> list[dict]:
    await ensure_indexes()

    reviews_col = _get_reviews_collection()
    reviews = [doc async for doc in reviews_col.find({})]
    if not reviews:
        logger.warning("[STEP4] reviews 컬렉션이 비어 있습니다.")
        return []

    groups = aggregate_by_product(reviews)
    skipped = len(reviews) - sum(g["mention_count"] for g in groups)
    logger.info(f"[STEP4] {len(reviews)}개 리뷰 중 품질 기준 미달 {skipped}개 제외, {len(groups)}개 상품으로 집계.")

    products = await _resolve_products(groups)
    await _save_products(products)
    return products


if __name__ == "__main__":
    asyncio.run(run())
